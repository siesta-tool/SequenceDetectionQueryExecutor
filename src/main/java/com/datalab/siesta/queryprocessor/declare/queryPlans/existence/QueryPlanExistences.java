package com.datalab.siesta.queryprocessor.declare.queryPlans.existence;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.DeclareUtilities;
import com.datalab.siesta.queryprocessor.declare.model.*;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryExistenceWrapper;
import com.datalab.siesta.queryprocessor.model.DBModel.EventTypes;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

import lombok.Setter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

@Component
@RequestScope
public class QueryPlanExistences implements QueryPlan {

    private final DeclareDBConnector declareDBConnector;
    private final JavaSparkContext javaSparkContext;
    private final SparkSession sparkSession;
    //initialize a protected variable of the required events
    private QueryResponseExistence queryResponseExistence;
    @Setter
    private Metadata metadata;

    private final DeclareUtilities declareUtilities;


    @Autowired
    public QueryPlanExistences(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                               DeclareUtilities declareUtilities, SparkSession sparkSession) {
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
        this.queryResponseExistence = new QueryResponseExistence();
        this.declareUtilities = declareUtilities;
        this.sparkSession = sparkSession;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryExistenceWrapper qew = (QueryExistenceWrapper) qw;
        //if existence, absence or exactly in modes
        Dataset<UniqueTracesPerEventType> uEventType = declareDBConnector.querySingleTableDeclare(metadata.getLogname());
        uEventType.persist(StorageLevel.MEMORY_AND_DISK());
        Map<String, HashMap<Integer, Long>> groupTimes = this.createMapForSingle(uEventType);
        //Make it available to the spark context
        Map<String, Long> singleUnique = this.extractUniqueTracesSingle(groupTimes);
        List<EventTypeOccurrences> uniqueSingleRows = singleUnique.entrySet().stream()
                .map(entry -> new EventTypeOccurrences(entry.getKey(), entry.getValue()))
                .toList();
        Dataset<EventTypeOccurrences> uniqueSingleDf = sparkSession.createDataset(uniqueSingleRows,
                Encoders.bean(EventTypeOccurrences.class));

        Dataset<UniqueTracesPerEventPair> uPairs = declareDBConnector.queryIndexTableDeclare(metadata.getLogname());
        uPairs.persist(StorageLevel.MEMORY_AND_DISK());
        Dataset<EventPairToNumberOfTrace> joined = joinUnionTraces(uPairs);
        joined.persist(StorageLevel.MEMORY_AND_DISK());

        Set<EventTypes> notFoundPairs = declareUtilities.extractNotFoundPairs(groupTimes.keySet(), joined);

        for (String m : qew.getModes()) {
            switch (m) {
                case "existence":
                    existence(groupTimes, qew.getSupport(), metadata.getTraces());
                    break;
                case "absence":
                    absence(groupTimes, qew.getSupport(), metadata.getTraces());
                    break;
                case "exactly":
                    exactly(groupTimes, qew.getSupport(), metadata.getTraces());
                    break;
                case "co-existence":
                    coExistence(joined, uniqueSingleDf, qew.getSupport(), metadata.getTraces());
                    break;
                case "not-co-existence":
                    notCoExistence(joined, uniqueSingleDf, qew.getSupport(), metadata.getTraces(), notFoundPairs);
                    break;
                case "choice":
                    choice(uEventType, qew.getSupport(), metadata.getTraces());
                    break;
                case "exclusive-choice":
                    exclusiveChoice(joined, uniqueSingleDf, singleUnique, qew.getSupport(), metadata.getTraces(), notFoundPairs);
                    break;
                case "responded-existence":
                    respondedExistence(joined, uniqueSingleDf,qew.getSupport(), metadata.getTraces());
                    break;
            }

        }

        // unpersist whatever needed
        joined.unpersist();
        uPairs.unpersist();
        uEventType.unpersist();
        return this.queryResponseExistence;
    }

    public QueryResponseExistence runAll(Map<String, HashMap<Integer, Long>> groupTimes, double support,
                                         Dataset<EventPairToNumberOfTrace> joined,
                                         long totalTraces, Map<String, Long> uniqueSingle,
                                         Dataset<UniqueTracesPerEventType> uEventType) {
        Set<EventTypes> notFoundPairs = declareUtilities.extractNotFoundPairs(groupTimes.keySet(),joined);

        //Make it available to the spark context
        List<EventTypeOccurrences> uniqueSingleRows = uniqueSingle.entrySet().stream()
                .map(entry -> new EventTypeOccurrences(entry.getKey(), entry.getValue()))
                .toList();
        Dataset<EventTypeOccurrences> uniqueSingleDf = sparkSession.createDataset(uniqueSingleRows,
                Encoders.bean(EventTypeOccurrences.class));

        //all event pairs will contain only those that weren't found in the dataset
        existence(groupTimes, support, metadata.getTraces());
        absence(groupTimes, support, metadata.getTraces());
        exactly(groupTimes, support, metadata.getTraces());
        coExistence(joined, uniqueSingleDf, support, totalTraces);
        choice(uEventType, support, totalTraces);
        exclusiveChoice(joined, uniqueSingleDf,uniqueSingle, support, totalTraces,notFoundPairs);
        respondedExistence(joined, uniqueSingleDf, support, totalTraces);
        return this.queryResponseExistence;
    }

    /**
     * Extracts a map from a UniqueTracesPerEventType RDD. This map has the form
     * (Event Type) -> (number of occurrences) -> # of traces that contain that much amount of occurrences of this
     * event type. e.g. searching how many traces have exactly 2 instances of the event type 'c'
     * ('c')->(2) -> response
     *
     * @param uEventType an RDD containing for each event type the unique traces and their corresponding occurrences
     *                   of this event type
     * @return a map of the form (Event Type) -> (number of occurrences) -> # of traces that contain that
     * much amount of occurrences of this event type.
     */
    public Map<String, HashMap<Integer, Long>> createMapForSingle(Dataset<UniqueTracesPerEventType> uEventType) {
        List<UniqueTracesPerEventType> uEventTypeList = uEventType.collectAsList();
        Map<String, HashMap<Integer, Long>> response = uEventTypeList.parallelStream().collect(Collectors.toMap(
                UniqueTracesPerEventType::getEventName, // Key: eventType (String)
                UniqueTracesPerEventType::groupTimes,   // Value: groupTimes() -> HashMap<Integer, Long>
                // Resolves key conflicts (shouldn't happen)
                (existing, replacement) -> existing
        ));
        return response;
    }

    /**
     * Based on the output of the above function, this code extracts the number of traces that contain a particular
     * event type, i.e. the response will contain information (event type) -> #traces containing it
     *
     * @param groupTimes a map of the form (Event Type) -> (number of occurrences) -> # of traces that contain that
     *                   * much amount of occurrences of this event type.
     * @return a map in the form (event type) -> #traces containing it
     */
    public Map<String, Long> extractUniqueTracesSingle(Map<String, HashMap<Integer, Long>> groupTimes) {
        Map<String, Long> response = new HashMap<>();
        groupTimes.entrySet().stream().map(x -> {
            long s = x.getValue().values().stream().mapToLong(l -> l).sum();
            return new Tuple2<>(x.getKey(), s);
        }).forEach(x -> response.put(x._1, x._2));
        return response;
    }

    /**
     * @param uPairs information extracted from the index table
     * @return a rdd of the type (eventA, eventB, traceId), i.e., which traces contain a specific event pair
     */
    public Dataset<EventPairToNumberOfTrace> joinUnionTraces(Dataset<UniqueTracesPerEventPair> uPairs) {

        // Create reversed key dataset (eventB, eventA)
        Dataset<Row> reversedPairs = uPairs
                .withColumnRenamed("eventA", "eventB-2") // Swap column names
                .withColumnRenamed("eventB", "eventA-2")
                .withColumnRenamed("uniqueTraces", "reversedUniqueTraces");

        // Perform Left Outer Join on (eventA, eventB) with (eventB, eventA)
        Dataset<Row> joinedPairs = uPairs
                .join(reversedPairs,
                        functions.col("eventA").equalTo(functions.col("eventA-2"))
                                .and(functions.col("eventB").equalTo(functions.col("eventB-2"))),
                        "left_outer")
                .select(functions.col("eventA"),
                        functions.col("eventB"),
                        functions.col("uniqueTraces"),
                        functions.col("reversedUniqueTraces"));

        Dataset<EventPairToNumberOfTrace> result = joinedPairs
                .withColumn("mergedTraces", functions.expr(
                        "array_union(uniqueTraces, reversedUniqueTraces)"
                ))
                .withColumn("numberOfTraces", functions.size(functions.col("mergedTraces")))
                .selectExpr("eventA", "eventB", "numberOfTraces")
                .as(Encoders.bean(EventPairToNumberOfTrace.class));

        return result;

    }


    /**
     * Extract the constraints that correspond to the 'existence' template.
     *
     * @param groupTimes  a map of the form (Event Type) -> (number of occurrences) -> # of traces that contain that
     *                    * much amount of occurrences of this event type.
     * @param support     minimum support that a pattern should have in order to be included in the result set
     * @param totalTraces the total number of traces in this log database
     */
    private void existence(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            List<Integer> times = new ArrayList<>(t.keySet()).stream().sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
            for (int time = 3; time > 0; time--) {
                int finalTime = time;
                double s = (double) times.stream().filter(x -> x >= finalTime).mapToLong(t::get).sum() / totalTraces;
                if (s >= support) {
                    response.add(new EventN(et, time, s));
                }
            }
        }
        this.queryResponseExistence.setExistence(response);
    }

    /**
     * Extract the constraints that correspond to the 'absence' template.
     *
     * @param groupTimes  a map of the form (Event Type) -> (number of occurrences) -> # of traces that contain that
     *                    * much amount of occurrences of this event type.
     * @param support     minimum support that a pattern should have in order to be included in the result set
     * @param totalTraces the total number of traces in this log database
     */
    private void absence(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            long totalSum = t.values().stream().mapToLong(x -> x).sum();
            t.put(0, totalTraces - totalSum);
            List<Integer> times = new ArrayList<>(t.keySet()).stream().sorted().collect(Collectors.toList());
            if (!times.contains(2)) times.add(2); //to be sure that it will run at least once
            for (int time = 3; time >= 2; time--) {
                int finalTime = time;
                double s = (double) times.stream().filter(x -> x < finalTime).map(t::get)
                        .filter(Objects::nonNull).mapToLong(x -> x).sum() / totalTraces;
                if (s >= support) {
                    response.add(new EventN(et, time, s));
                }
            }
        }
        this.queryResponseExistence.setAbsence(response);
    }

    /**
     * Extract the constraints that correspond to the 'exactly' template.
     *
     * @param groupTimes  a map of the form (Event Type) -> (number of occurrences) -> # of traces that contain that
     *                    * much amount of occurrences of this event type.
     * @param support     minimum support that a pattern should have in order to be included in the result set
     * @param totalTraces the total number of traces in this log database
     */
    private void exactly(Map<String, HashMap<Integer, Long>> groupTimes, double support, long totalTraces) {
        Set<String> eventTypes = groupTimes.keySet();
        List<EventN> response = new ArrayList<>();
        for (String et : eventTypes) { //for every event type
            HashMap<Integer, Long> t = groupTimes.get(et);
            long totalSum = t.values().stream().mapToLong(x -> x).sum();
            if (!t.containsKey(0)) t.put(0, totalTraces - totalSum);
            for (Map.Entry<Integer, Long> x : t.entrySet()) {
                if (x.getValue() >= (support * totalTraces) && x.getKey() > 0) {
                    response.add(new EventN(et, x.getKey(), x.getValue().doubleValue() / totalTraces));
                }
            }
        }
        this.queryResponseExistence.setExactly(response);
    }

    /**
     * Extract the constraints that correspond to the 'exactly' template.
     *
     * @param joinedUnion   A Dataset that contains objects of the form (eventA,eventB,traceID), i.e. which traces
     *                      contain at least one occurrence of the pair (eventA, eventB)
     * @param uniqueSingleDf A Dataset that contains a map of the form (event type) -> # traces
     *                      that contain this event type
     * @param support      the user-defined support
     * @param totalTraces  Total traces in this log database
     */
    private void coExistence(Dataset<EventPairToNumberOfTrace> joinedUnion,
                             Dataset<EventTypeOccurrences> uniqueSingleDf, double support, long totalTraces) {

        // |A| = |IndexTable(a,b) U IndexTable(b,a)|, i.e., unique traces where a and b co-exist
        // total_traces = |A| + (non-of them exist) + (only 'a' exist) + (only b exist) (1)
        // (only 'a' exists) = (unique traces of 'a') - (traces that a co-existed with b)
        // where the co-existence is true is when |A|+(non-of them exist) >= support (2)
        // (1)+(2)=> total_traces - (unique traces of a) + |A| - (unique traces of b) + |A| >= support* total_traces
        //  total_traces - (unique traces of a) - (unique traces of b) - |A| >= support* total_traces
        Dataset<EventPairToNumberOfTrace> initialFiltered = joinedUnion
                .filter(functions.col("eventA").notEqual(functions.col("eventB"))) // Remove self pairs
                .filter(functions.col("eventA").leq(functions.col("eventB"))) // Order pairs
                .filter(functions.col("numberOfTraces").geq(support * totalTraces)); // Minimum support check

        Dataset<Row> joinedWithUnique = initialFiltered
                .join(uniqueSingleDf.withColumnRenamed("eventName", "eventA")
                        .withColumnRenamed("numberOfTraces", "uniqueA"), "eventA", "left")
                .join(uniqueSingleDf.withColumnRenamed("eventName", "eventB")
                        .withColumnRenamed("numberOfTraces", "uniqueB"), "eventB", "left");

        Dataset<Row> filteredAndComputed = joinedWithUnique
                .withColumn("computedSupport", functions.expr(
                        totalTraces + " - uniqueA - uniqueB + 2 * numberOfTraces"
                ))
                .filter(functions.col("computedSupport").geq(support * totalTraces)) // Filter based on computed support
                .withColumn("support", functions.col("computedSupport").divide(totalTraces)) // Normalize support
                .select("eventA", "eventB", "support");

        List<EventPairSupport> coExistence = filteredAndComputed
                .as(Encoders.bean(EventPairSupport.class))
                .collectAsList();
        queryResponseExistence.setCoExistence(coExistence);
    }

    private void notCoExistence(Dataset<EventPairToNumberOfTrace> joinedUnion,
                                Dataset<EventTypeOccurrences> uniqueSingleDf, double support, long totalTraces,
                                Set<EventTypes> notFound) {
        //valid event types can be used as first in a pair (since they have support greater than the user-defined)
        Dataset<EventPairToNumberOfTrace> initialFiltered = joinedUnion
                .filter(functions.col("eventA").notEqual(functions.col("eventB"))) // Remove self pairs
                .filter(functions.col("eventA").leq(functions.col("eventB"))) // Order pairs
                .filter(functions.col("numberOfTraces").leq(1-support * totalTraces)); // Minimum support check

        List<EventPairSupport> notCoExistence =initialFiltered
                .selectExpr("eventA", "eventB", String.format("1-numberOfTraces/%s as support", totalTraces))
                .as(Encoders.bean(EventPairSupport.class))
                .collectAsList();

        //Add all the pairs in the notFound that their reverse is also in this set. Meaning that these two
        //events never co-exist in the entire database
        Set<EventPairSupport> notCoExist = new HashSet<>();
        for (EventTypes ep : notFound) {
            if (notFound.contains(new EventTypes(ep.getEventB(), ep.getEventA()))) {
                if (ep.getEventA().compareTo(ep.getEventB()) > 0) {
                    notCoExist.add(new EventPairSupport(ep.getEventA(), ep.getEventB(), 1));
                } else {
                    notCoExist.add(new EventPairSupport(ep.getEventB(), ep.getEventA(), 1));
                }
            }
        }
        // add all findings in one list
        List<EventPairSupport> response = new ArrayList<>();
        response.addAll(notCoExistence);
        response.addAll(notCoExist);
        queryResponseExistence.setNotCoExistence(response);
    }

    /**
     * Extract the constraints that correspond to the 'choice' template.
     *
     * @param uEventType   a Dataset in the form (event type, [(traceId,#occurrences)])
     * @param support     the user-defined support
     * @param totalTraces Total traces in this log database
     */
    private void choice(Dataset<UniqueTracesPerEventType> uEventType, double support, long totalTraces) {
        //create possible pairs without duplication

        Dataset<Row> eventPairs = uEventType.alias("a")
                .crossJoin(uEventType.alias("b")) // Generates ALL event combinations
                // Avoid duplicate pairs (A, B) & (B, A)
                .filter(functions.col("a.eventName").lt(functions.col("b.eventName")))
                .select(
                        functions.col("a.eventName").alias("eventA"),
                        functions.col("b.eventName").alias("eventB"),
                        functions.col("a.occurrences").alias("occurrencesA"),
                        functions.col("b.occurrences").alias("occurrencesB")
                );

        // Apply early pruning: only keep event pairs where the total occurrences meet the threshold
        Dataset<Row> filteredPairs = eventPairs
                .withColumn("totalOccurrences", functions.expr("size(occurrencesA) + size(occurrencesB)"))
                .filter(functions.col("totalOccurrences").geq(support * totalTraces)); // Early filtering

        //actual count the traces in which either of them exists (removing the duplicate counts - traces where
        //both exist)
        Dataset<EventPairSupport> calculateSupport = filteredPairs
                .withColumn("uniqueTraces", functions.expr(
                        "array_union(transform(occurrencesA, x -> x.traceId), transform(occurrencesB, x -> x.traceId))"
                ))
                .withColumn("traceCount", functions.size(functions.col("uniqueTraces"))) // Count unique traces
                .withColumn("support", functions.col("traceCount").divide(totalTraces)) // Compute support
                .filter(functions.col("support").geq(support)) // Final filtering based on support
                .select("eventA", "eventB", "support")
                .as(Encoders.bean(EventPairSupport.class));

        List<EventPairSupport> choice = calculateSupport.collectAsList();
        queryResponseExistence.setChoice(choice);
    }


    /**
     * Extract the constraints that correspond to the 'exclusive choice' template.
     *
     * @param joinedUnion        A Dataset that contains objects of the form (eventA,eventB,traceID), i.e. which traces
     *                      contain at least one occurrence of the pair (eventA, eventB)
     * @param uniqueSingleDf A Dataset that contains a map of the form (event type) -> # traces
     *      *                      that contain this event type
     * @param uniqueSingle The same as uniqueSingleDF but located in master
     * @param support      the user-defined support
     * @param totalTraces  Total traces in this log database
     * @param notFound      A set of the event pairs that have 0 occurrence in the log database
     */
    private void exclusiveChoice(Dataset<EventPairToNumberOfTrace> joinedUnion, Dataset<EventTypeOccurrences> uniqueSingleDf,
                                 Map<String,Long> uniqueSingle,double support, long totalTraces, Set<EventTypes> notFound) {

        Dataset<EventPairToNumberOfTrace> initialFiltered = joinedUnion
                .filter(functions.col("eventA").notEqual(functions.col("eventB"))) // Remove self pairs
                .filter(functions.col("eventA").leq(functions.col("eventB"))); // Order pairs

        Dataset<Row> joinedWithUnique = initialFiltered
                .join(uniqueSingleDf.withColumnRenamed("eventName", "eventA")
                        .withColumnRenamed("numberOfTraces", "uniqueA"), "eventA", "left")
                .join(uniqueSingleDf.withColumnRenamed("eventName", "eventB")
                        .withColumnRenamed("numberOfTraces", "uniqueB"), "eventB", "left");

        // detects exclusive choice in pairs that appear at least once in the log database
        Dataset<EventPairSupport> exclusiveChoiceDF = joinedWithUnique
                .withColumn("support", functions.expr(
                        "(uniqueA + uniqueB - 2 * numberOfTraces) / " + totalTraces
                ))
                .filter(functions.col("support").geq(support))
                .select("eventA","eventB","support")
                .as(Encoders.bean(EventPairSupport.class));

        List<EventPairSupport> exclusiveChoice = exclusiveChoiceDF.collectAsList();

        // detects exclusive choice in pairs that do not appear in the log database
        // therefore it checks if both (a,b) and (b,a) are in the notFound set
        Set<EventPairSupport> notCoExist = new HashSet<>();
        for (EventTypes ep : notFound) {
            if (notFound.contains(new EventTypes(ep.getEventB(), ep.getEventA()))) {
                if (ep.getEventA().compareTo(ep.getEventB()) > 0) {
                    notCoExist.add(new EventPairSupport(ep.getEventA(), ep.getEventB(), 1));
                } else {
                    notCoExist.add(new EventPairSupport(ep.getEventB(), ep.getEventA(), 1));
                }
            }
        }



        //Calculates the support of the constraints detected from the not found pairs, as this behavior
        //should describe at least 'support'% of the total traces
        List<EventPairSupport> exclusiveChoice2 = notCoExist.stream().map(x -> {
                    double sup = (double) (uniqueSingle.get(x.getEventA()) +
                           uniqueSingle.get(x.getEventB())) / totalTraces;
                    return new EventPairSupport(x.getEventA(), x.getEventB(), sup);
                }).filter(x -> x.getSupport() >= support)
                .collect(Collectors.toList());

        //add both together and pass them to the response
        exclusiveChoice2.addAll(exclusiveChoice);
        queryResponseExistence.setExclusiveChoice(exclusiveChoice2);
    }


    /**
     * Extract the constraints that correspond to the 'responeddexistence' template.
     *
     * @param joinedUnion        A Dataset that contains objects of the form (eventA,eventB,traceID), i.e. which traces
     *                      contain at least one occurrence of the pair (eventA, eventB)
     * @param uniqueSingleDf A Dataset that contains a map of the form (event type) -> # traces
     *      *                      that contain this event type
     * @param support      the user-defined support
     * @param totalTraces  Total traces in this log database
     */

    private void respondedExistence(Dataset<EventPairToNumberOfTrace> joinedUnion, Dataset<EventTypeOccurrences> uniqueSingleDf,
                                   double support, long totalTraces) {
        Dataset<EventPairToNumberOfTrace> initialFiltered = joinedUnion
                .filter(functions.col("eventA").notEqual(functions.col("eventB"))); // Remove self pairs

        Dataset<Row> joinedWithUnique = initialFiltered
                .join(uniqueSingleDf.withColumnRenamed("eventName", "eventA")
                        .withColumnRenamed("numberOfTraces", "uniqueA"), "eventA", "left")
                .join(uniqueSingleDf.withColumnRenamed("eventName", "eventB")
                        .withColumnRenamed("numberOfTraces", "uniqueB"), "eventB", "left");

        // detects exclusive choice in pairs that appear at least once in the log database
        Dataset<Row> extractedBothSupports = joinedWithUnique
                .withColumn("supportA", functions.expr(
                        String.format("(numberOfTraces + %s - uniqueA)/%s",totalTraces,totalTraces)
                ))
                .withColumn("supportB", functions.expr(
                        String.format("(numberOfTraces + %s - uniqueB)/%s",totalTraces,totalTraces)
                ));

        Dataset<EventPairSupport> eventsForward = extractedBothSupports
                .filter(functions.col("supportA").geq(support))
                .selectExpr("eventA","eventB","supportA as support")
                .as(Encoders.bean(EventPairSupport.class));

        Dataset<EventPairSupport> eventsBackwards = extractedBothSupports
                .filter(functions.col("supportB").geq(support))
                .selectExpr("eventB as eventA","eventA as eventB","supportB as support")
                .as(Encoders.bean(EventPairSupport.class));


        List<EventPairSupport> responseExistence = eventsForward.union(eventsBackwards)
                .distinct().collectAsList();

        //pass to the response
        this.queryResponseExistence.setRespondedExistence(responseExistence);

    }

    public void initResponse() {
        this.queryResponseExistence = new QueryResponseExistence();
    }

}
