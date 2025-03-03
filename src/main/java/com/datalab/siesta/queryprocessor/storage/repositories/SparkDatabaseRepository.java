package com.datalab.siesta.queryprocessor.storage.repositories;

import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Events.*;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This in a class the contains common logic for all databases that utilize spark (like Cassandra and S3)
 * and therefore it is implemented by both of them
 */
public abstract class SparkDatabaseRepository implements DatabaseRepository {

    protected SparkSession sparkSession;

    protected JavaSparkContext javaSparkContext;

    protected Utils utils;

    @Autowired
    public SparkDatabaseRepository(SparkSession sparkSession, JavaSparkContext javaSparkContext, Utils utils) {
        this.sparkSession = sparkSession;
        this.javaSparkContext = javaSparkContext;
        this.utils = utils;
    }

    /**
     * return all the IndexPairs grouped by the eventA and eventB
     * needs to be implemented by each different connector
     *
     * @param pairs   set of the pairs
     * @param logname the log database
     * @return extract the pairs
     */
    protected Dataset<IndexPair> getAllEventPairs(Set<EventPair> pairs,
                                                  String logname, Metadata metadata, Timestamp from, Timestamp till) {
        return null;
    }

    /**
     * return all the IndexPairs grouped by the eventA and eventB
     * needs to be implemented by each different connector
     *
     * @param pairs   set of the pairs
     * @param logname the log database
     * @return extract the pairs
     */
    protected Dataset<IndexPair> getAllEventPairs(Set<EventPair> pairs, String logname) {
        return null;
    }

    /**
     * Return all events in the SequenceTable as a dataset in order to be filtered latter
     * needs to be implemented by each different connector
     * @param logname  the log database
     * @return  all events in the SequenceTable
     */
    protected Dataset<EventModel> readSequenceTable(String logname){
        return null;
    }

    /**
     * Retrieves the appropriate events from the SequenceTable, which contains the original traces
     *
     * @param logname  the log database
     * @param traceIds the ids of the traces that will be retrieved
     * @return a map where the key is the trace id and the value is a list of the retrieved events (with their
     * * timestamps)
     */
    @Override
    public Map<String, List<EventBoth>> querySeqTable(String logname, List<String> traceIds) {
        Dataset<EventModel> eventsDF = this.readSequenceTable(logname)
                .filter(functions.col("traceId").isin(traceIds.toArray()));
        return this.transformEventModelToMap(eventsDF.toDF());
    }

    /**
     * Retrieves the appropriate events from the SequenceTable, which contains the original traces
     *
     * @param logname    the log database
     * @param traceIds   the ids of the traces that will be retrieved
     * @param eventTypes the events that will be retrieved
     * @param from       the starting timestamp, set to null if not used
     * @param till       the ending timestamp, set to null if not used
     * @return a map where the key is the trace id and the value is a list of the retrieved events (with their
     * timestamps)
     */
    @Override
    public Map<String, List<EventBoth>> querySeqTable(String logname, List<String> traceIds, Set<String> eventTypes,
                                                      Timestamp from, Timestamp till) {
        Dataset<EventModel> eventsDF = this.readSequenceTable(logname);
        //filter based on id and based on eventType
        eventsDF = eventsDF.filter(functions.col("traceId").isin(traceIds.toArray()))
                .filter(functions.col("eventName").isin(eventTypes.toArray()));
        //filter based on the timestamps and the parameters from and till
        Dataset<Row> filteredTimestamps = eventsDF
                .withColumn("timestamp-true", functions.col("timestamp").cast("timestamp"));
        if(from !=null){
            filteredTimestamps = filteredTimestamps.filter(functions.col("timestamp-true").geq(from));
        }
        if(till!=null){
            filteredTimestamps = filteredTimestamps.filter(functions.col("timestamp-true").leq(till));
        }

        return this.transformEventModelToMap(filteredTimestamps);
    }

    private Map<String,List<EventBoth>> transformEventModelToMap(Dataset<Row> events){
        List<EventModel> eventsList = events
                .select("traceId","eventName","timestamp","position")
                .as(Encoders.bean(EventModel.class))
                .collectAsList();

        Map<String, List<EventBoth>> response = eventsList.parallelStream()
                .map(x->
                        new EventBoth(x.getEventName(),x.getTraceId(),Timestamp.valueOf(x.getTimestamp()),x.getPosition()))
                .collect(Collectors.groupingByConcurrent(EventBoth::getTraceID));
        return response;
    }

    /**
     * This function reads data from the Sequence table into a JavaRDD, any database that utilizes spark should
     * override it
     *
     * @param logname   Name of the log
     * @param bTraceIds broadcasted the values of the trace ids we are interested in
     * @return a JavaRDD<Trace>
     */
    protected Dataset<Trace> querySequenceTablePrivate(String logname, Broadcast<Set<String>> bTraceIds) {
        return null;
    }

    /**
     * Retrieves data from the primary inverted index
     *
     * @param pairs   a set of the pairs that we need to retrieve information for
     * @param logname the log database
     * @return the corresponding records from the index
     */
    @Override
    public IndexRecords queryIndexTable(Set<EventPair> pairs, String logname) {
        Dataset<IndexPair> results = this.getAllEventPairs(pairs, logname);
        return this.transformToIndexRecords(results);
    }

    /**
     * Retrieves data from the primary inverted index
     *
     * @param pairs    a set of the pairs that we need to retrieve information for
     * @param logname  the log database
     * @param metadata the metadata for this log database
     * @param from     the starting timestamp, set to null if not used
     * @param till     the ending timestamp, set to null if not used
     * @return the corresponding records from the index
     */
    @Override
    public IndexRecords queryIndexTable(Set<EventPair> pairs, String logname, Metadata metadata, Timestamp from, Timestamp till) {
        Dataset<IndexPair> results = this.getAllEventPairs(pairs, logname, metadata, from, till);
        return this.transformToIndexRecords(results);
    }

    public IndexRecords transformToIndexRecords(Dataset<IndexPair> indexPairs) {
        Dataset<Row> groupedDf = indexPairs.withColumn("indexPair",
                        functions.struct("trace_id", "eventA", "eventB", "timestampA",
                                "timestampB", "positionA", "positionB"))
                .groupBy("eventA", "eventB")
                .agg(functions.collect_list("indexPair").alias("indexPairs"))
                .select("traceId", "events");

        // Convert DataFrame to Map<String, List<Event>>
        Map<EventTypes, List<IndexPair>> eventsMap = groupedDf
                .collectAsList()
                .stream()
                .collect(Collectors.toMap(
                        row -> new EventTypes(row.getString(0), row.getString(1)),
                        row -> {
                            List<Row> eventRows = row.getList(2);
                            return eventRows.stream()
                                    .map(eventRow -> new IndexPair(
                                            eventRow.getString(0),
                                            eventRow.getString(1),
                                            eventRow.getString(2),
                                            eventRow.getString(3),
                                            eventRow.getString(4),
                                            eventRow.getInt(5),
                                            eventRow.getInt(6)
                                    ))
                                    .collect(Collectors.toList());
                        }
                ));
        return new IndexRecords(eventsMap);
    }

    /**
     * Extract the ids of the traces that contains all the provided pairs
     *
     * @param pairs          pairs retrieved from the storage
     * @param trueEventPairs the pairs that required to appear in a trace in order to be a candidate
     * @return the candidate trace ids
     */
    protected List<String> getCommonIds(Dataset<IndexPair> pairs, Set<EventPair> trueEventPairs) {
        Set<EventTypes> truePairs = trueEventPairs.stream()
                .map(x -> new EventTypes(x.getEventA().getName(), x.getEventB().getName()))
                .collect(Collectors.toSet());
        String eventFilter = truePairs.stream()
                .map(x -> String.format("(eventA = '%s' AND eventB = '%s')", x.getEventA(), x.getEventB()))
                .collect(Collectors.joining(" OR "));

        // Next sequence extract the ids of the traces that contains all the required et-pairs (iun the truePairs list)
        List<String> listOfTraces = pairs
                .select("eventA", "eventB", "trace_id") // Maintain the required fields
                .distinct() // Remove duplicates
                .where(eventFilter) // Maintain only the et-pairs in the true pairs
                .groupBy("trace_id") // Group based on the trace id
                .agg(functions.count("*").alias("valid_et_pairs")) // Count how many of the valid pairs each trace has
                .filter(functions.col("valid_et_pairs").equalTo(truePairs.size()))// Keeps thr traces that have all valid et-pairs
                .select("trace_id") // Return the trace_ids
                .as(Encoders.STRING()) //Transform to string
                .collectAsList(); //Collect as List
        return listOfTraces;
    }

    /**
     * Filters te results based on the starting and ending timestamp
     *
     * @param pairs    the retrieved pairs from the IndexTable
     * @param traceIds the trace ids that contains all the required pairs
     * @param from     the starting timestamp, set to null if not used
     * @param till     the ending timestamp, set to null if not used
     * @return the intermediate results, i.e. the candidate traces before remove false positives
     */
    protected IndexMiddleResult addFilterIds(Dataset<IndexPair> pairs, List<String> traceIds, Timestamp from, Timestamp till) {
        IndexMiddleResult imr = new IndexMiddleResult();
        imr.setTrace_ids(traceIds);
        // Filter to maintain only the pruned traces
        Dataset<IndexPair> filteredDf = pairs.filter(functions.col("trace_id").isin(traceIds.toArray()));
        // Extract EventBoth from the IndexPair
        Dataset<EventModel> eventsDf = this.getEventsFromIndexRecords(filteredDf);

        // Check if timestamp filtering is needed
        boolean filterByTime = from != null || till != null;

        if (filterByTime) {
            Dataset<Row> rows = eventsDf.withColumn("timestamp-2",
                    functions.to_timestamp(functions.col("timestamp"), "yyyy-MM-dd HH:mm:ss"));
            if (from != null) {
                rows = rows.filter(
                        functions.col("timestamp-2").isNull().or(
                                functions.col("timestamp-2").geq(from)));
            }
            if (till != null) {
                rows = rows.filter(
                        functions.col("timestamp-2").isNull().or(
                                functions.col("timestamp-2").leq(till)));
            }
            eventsDf = rows.select("eventName", "traceId", "position", "timestamp")
                    .as(Encoders.bean(EventModel.class));
        }

        Dataset<Row> groupedDf = eventsDf
                .withColumn("event", functions.struct("traceId", "eventName", "position", "timestamp"))
                .groupBy("traceId")
                .agg(functions.collect_list("event").alias("events"))
                .select("traceId", "events");

        // Convert DataFrame to Map<String, List<Event>>
        Map<String, List<Event>> eventsMap = groupedDf
                .collectAsList()
                .stream()
                .collect(Collectors.toMap(
                        row -> row.getString(0),
                        row -> {
                            List<Row> eventRows = row.getList(1);
                            return eventRows.stream()
                                    .map(eventRow -> {
                                        if (eventRow.getString(3) == null) {
                                            return new EventPos(
                                                    eventRow.getString(1), // eventName
                                                    eventRow.getString(0), // traceId
                                                    eventRow.getInt(2)    // position
                                            );
                                        } else {
                                            return new EventTs(
                                                    eventRow.getString(1), // eventName
                                                    eventRow.getString(0), // traceId
                                                    Timestamp.valueOf(eventRow.getString(3))// timestamp
                                            );
                                        }
                                    })
                                    .collect(Collectors.toList());
                        }
                ));

        eventsMap.forEach((key, eventList) -> Collections.sort(eventList));
        imr.setEvents(eventsMap);

        return imr;

    }


    /**
     * Detects the traces that contain all the given event pairs
     *
     * @param logname  the log database
     * @param combined a list where each event pair is combined with the according stats from the CountTable
     * @param metadata the log database metadata
     * @param expairs  the event pairs extracted from the query
     * @param from     the starting timestamp, set to null if not used
     * @param till     the ending timestamp, set to null if not used
     * @return the traces that contain all the pairs. It will be then processed by SASE in order to remove false
     * positives.
     */
    @Override
    public IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined, Metadata metadata,
                                                     ExtractedPairsForPatternDetection expairs, Timestamp from, Timestamp till) {
        Set<EventPair> pairs = combined.stream().map(x -> x._1).collect(Collectors.toSet());
        Dataset<IndexPair> indexPairs = this.getAllEventPairs(pairs, logname, metadata, from, till);
        indexPairs.persist(StorageLevel.MEMORY_AND_DISK());
        List<String> traces = this.getCommonIds(indexPairs, expairs.getTruePairs());
        IndexMiddleResult imr = this.addFilterIds(indexPairs, traces, from, till);
        indexPairs.unpersist();
        return imr;
    }

    /**
     * Should be overridden by any storage that uses spark
     *
     * @param logname    The name of the Log
     * @param traceIds   The traces we want to detect
     * @param eventTypes The event types to be collected
     * @return a JavaRDD<EventBoth> that will be used in querySingleTable and querySingleTableGroups
     */
    protected Dataset<EventBoth> getFromSingle(String logname, Set<String> traceIds, Set<String> eventTypes) {
        Broadcast<Set<String>> bTraces = javaSparkContext.broadcast(traceIds);
        //TODO: implement this when needed
        return null;
//        return queryFromSingle(logname, eventTypes).filter((Function<EventBoth, Boolean>) event ->
//                bTraces.getValue().contains(event.getTraceID()));
    }

    protected JavaRDD<EventBoth> queryFromSingle(String logname, Set<String> eventTypes) {
        return null;
    }

    @Override
    public Map<String, List<EventBoth>> querySingleTable(String logname, Set<String> eventTypes) {
        JavaRDD<EventBoth> events = queryFromSingle(logname, eventTypes);
        JavaPairRDD<String, Iterable<EventBoth>> pairs = events.groupBy((Function<EventBoth, String>) Event::getName);
        return pairs.mapValues((Function<Iterable<EventBoth>, List<EventBoth>>) e -> {
            List<EventBoth> tempList = new ArrayList<>();
            for (EventBoth ev : e) {
                tempList.add(ev);
            }
            return tempList;
        }).collectAsMap();
    }

    /**
     * Retrieves the appropriate events from the SingleTable, which contains the single inverted index
     *
     * @param logname    the log database
     * @param traceIds   the ids of the traces that wil be retrieved
     * @param eventTypes the events that will we retrieved
     * @return a list of all the retrieved events (wth their timestamps)
     */
    @Override
    public List<EventBoth> querySingleTable(String logname, Set<String> traceIds, Set<String> eventTypes) {
        //TODO: implement this when needed
        return null;
//        return this.getFromSingle(logname, traceIds, eventTypes).collect();
    }

    /**
     * Retrieves the appropriate events from the SingleTable, which contains the single inverted index
     *
     * @param logname    the log database
     * @param groups     a list of the groups as defined in the query
     * @param eventTypes the events that will we retrieved
     * @return a map where the key is the group id and the value is a list of the retrieved events (with their t
     * imestamps)
     */
    @Override
    public Map<Integer, List<EventBoth>> querySingleTableGroups(String logname, List<Set<String>> groups, Set<String> eventTypes) {
        //TODO: implement this when needed
        return null;
//        Set<String> allTraces = groups.stream()
//                .flatMap((java.util.function.Function<Set<String>, Stream<String>>) Collection::stream)
//                .collect(Collectors.toSet());
//        Broadcast<List<Set<String>>> bgroups = javaSparkContext.broadcast(groups);
//        Broadcast<Integer> bEventTypesSize = javaSparkContext.broadcast(eventTypes.size());
//        JavaRDD<EventBoth> eventRDD = this.getFromSingle(logname, allTraces, eventTypes);
//        Map<Integer, List<EventBoth>> response = eventRDD.map((Function<EventBoth, Tuple2<Integer, EventBoth>>) event -> {
//                    for (int g = 0; g < bgroups.value().size(); g++) {
//                        if (bgroups.value().get(g).contains(event.getTraceID())) return new Tuple2<>(g + 1, event);
//                    }
//                    return new Tuple2<>(-1, event);
//                })
//                .filter((Function<Tuple2<Integer, EventBoth>, Boolean>) event -> event._1 != -1)
//                .groupBy((Function<Tuple2<Integer, EventBoth>, Integer>) event -> event._1)
//                //maintain only these groups that contain all the event types in the query
//                .filter((Function<Tuple2<Integer, Iterable<Tuple2<Integer, EventBoth>>>, Boolean>) group -> {
//                    Set<String> events = new HashSet<>();
//                    group._2.forEach(x -> events.add(x._2.getName()));
//                    return events.size() == bEventTypesSize.value();
//                })
//                .mapValues((Function<Iterable<Tuple2<Integer, EventBoth>>, List<EventBoth>>) group -> {
//                    List<EventBoth> eventBoth = new ArrayList<>();
//                    for (Tuple2<Integer, EventBoth> e : group) {
//                        eventBoth.add(e._2);
//                    }
//                    return eventBoth.stream().sorted().collect(Collectors.toList());
//                }).collectAsMap();
//        return response;
    }

    /**
     * This method transforms the rows read from the Database to IndexPair. Since SIESTA supports both timestamp
     * and positions, this method is responsible to extract the schema and make the corresponding changes. Finally,
     * since this is the place that identifies if there are timestamps in the index, we have also included the
     * from/till filtering
     *
     * @param indexRows
     * @return
     */
    protected Dataset<IndexPair> transformToIndexPairSet(Dataset<Row> indexRows, Timestamp from, Timestamp till) {
        StructType schema = indexRows.schema();
        // Check if each column exists before selecting it
        boolean hasTimestampA = Arrays.asList(schema.fieldNames()).contains("timestampA");
        boolean hasTimestampB = Arrays.asList(schema.fieldNames()).contains("timestampB");
        boolean hasPositionA = Arrays.asList(schema.fieldNames()).contains("positionA");
        boolean hasPositionB = Arrays.asList(schema.fieldNames()).contains("positionB");

        Column traceId = functions.col("trace_id");
        Column eventA = functions.col("eventA");
        Column eventB = functions.col("eventB");

        //here is the filtering for the till and from if the indexing has been done using timestamp
        if (hasTimestampA && hasTimestampB) {
            indexRows = indexRows
                    .withColumn("timestampA", functions.to_timestamp(functions.col("timestampA"), "yyyy-MM-dd HH:mm:ss"))
                    .withColumn("timestampB", functions.to_timestamp(functions.col("timestampB"), "yyyy-MM-dd HH:mm:ss"));

            if (from != null) {
                indexRows = indexRows.filter(functions.col("timestampA").isNull()
                        .or(functions.col("timestampA").geq(from)));
            }
            if (till != null) {
                indexRows = indexRows.filter(functions.col("timestampB").isNull()
                        .or(functions.col("timestampB").leq(till)));
            }
        }
        Column timestampA = hasTimestampA ? functions.col("timestampA") : functions.lit(null).cast("string");
        Column timestampB = hasTimestampB ? functions.col("timestampB") : functions.lit(null).cast("string");
        Column positionA = hasPositionA ? functions.col("positionA") : functions.lit(null).cast("int");
        Column positionB = hasPositionB ? functions.col("positionB") : functions.lit(null).cast("int");

        Dataset<IndexPair> indexPairDataset = indexRows.select(traceId, eventA, eventB, timestampA.alias("timestampA"),
                        timestampB.alias("timestampB"), positionA.alias("positionA"),
                        positionB.alias("positionB"))
                .as(Encoders.bean(IndexPair.class));
        return indexPairDataset;
    }

    private Dataset<EventModel> getEventsFromIndexRecords(Dataset<IndexPair> indexPairs) {
        Dataset<Row> eventA_DF = indexPairs
                .selectExpr(
                        "eventA as eventName",
                        "timestampA as timestamp",
                        "positionA as position",
                        "trace_id as traceId"
                );
        Dataset<Row> eventB_DF = indexPairs
                .selectExpr(
                        "eventB as eventName",
                        "timestampB as timestamp",
                        "positionB as position",
                        "trace_id as traceId"
                );
        Dataset<EventModel> eventsDF = eventA_DF.union(eventB_DF)
                .distinct()
                .as(Encoders.bean(EventModel.class));
        return eventsDF;
    }
    //Below are for Declare//


}
