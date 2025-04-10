package com.datalab.siesta.queryprocessor.storage.repositories;

import com.datalab.siesta.queryprocessor.declare.model.*;
import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Events.*;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;
import com.datalab.siesta.queryprocessor.storage.model.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Tuple2;

import javax.xml.crypto.Data;
import java.sql.Timestamp;
import java.util.*;
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
     * Return all events in the SequenceTable as a dataset in order to be filtered latter
     * needs to be implemented by each different connector
     *
     * @param logname the log database
     * @return all events in the SequenceTable
     */
    protected Dataset<EventModel> readSequenceTable(String logname) {
        return null;
    }

    protected Dataset<EventModelAttributes> readSequenceTableAttributes(String logname, Set<String> chosen_attributes) {return null;}
    /**
     * Return all events in the SequenceTable as a dataset in order to be filtered latter
     * needs to be implemented by each different connector
     *
     * @param logname the log database
     * @return all events in the SequenceTable
     */
    protected Dataset<EventModel> readSingleTable(String logname) {
        return null;
    }

    /**
     * Return all events in the CountTable as a dataset in order to be filtered latter
     * needs to be implemented by each different connector
     *
     * @param logname the log database
     * @return all events in the CountTable
     */
    protected Dataset<Count> readCountTable(String logname) {
        return null;
    }

    /**
     * Return all events in the IndexTable as a dataset in order to be filtered latter
     * needs to be implemented by each different connector
     *
     * @param logname the log database
     * @return all events in the IndexTable
     */
    protected Dataset<IndexPair> readIndexTable(String logname) {
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
    protected Dataset<IndexPair> getAllEventPairs(Set<EventPair> pairs,
                                                  String logname, Metadata metadata, Timestamp from, Timestamp till) {
        String filter = pairs.stream().map(x -> new Tuple2<>(x.getEventA().getName(), x.getEventB().getName()))
                .collect(Collectors.toSet())
                .stream().map(x -> String.format("(eventA = '%s' and eventB = '%s')", x._1(), x._2()))
                .collect(Collectors.joining(" or "));
        Dataset<IndexPair> indexDataset = readIndexTable(logname)
                .where(filter); //filter based on events

        if (!metadata.getMode().equals("position")) { // we can filter based on the timestamp also
            Dataset<Row> indexRows = indexDataset
                    .withColumn("timestampA-2", functions.to_timestamp(functions.col("timestampA"), "yyyy-MM-dd HH:mm:ss"))
                    .withColumn("timestampB-2", functions.to_timestamp(functions.col("timestampB"), "yyyy-MM-dd HH:mm:ss"));

            if (from != null) {
                indexRows = indexRows.filter(functions.col("timestampA-2").isNull()
                        .or(functions.col("timestampA-2").geq(from)));
            }
            if (till != null) {
                indexRows = indexRows.filter(functions.col("timestampB-2").isNull()
                        .or(functions.col("timestampB-2").leq(till)));
            }
            indexDataset = indexRows.select("trace_id", "eventA", "eventB", "timestampA", "timestampB", "positionA", "positionB", "attributesA", "attributesB")
                    .as(Encoders.bean(IndexPair.class));
        }

        return indexDataset;
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

    @Override
    public Map<String, List<EventBoth>> querySeqTableAttributes(String logname, List<String> traceIds, Set<String> chosen_attributes) {
        Dataset<EventModelAttributes> eventsDF = this.readSequenceTableAttributes(logname, chosen_attributes)
                .filter(functions.col("traceId").isin(traceIds.toArray()));
        return this.transformEventModelAttributesToMap(eventsDF.toDF());
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
        if (from != null) {
            filteredTimestamps = filteredTimestamps.filter(functions.col("timestamp-true").geq(from));
        }
        if (till != null) {
            filteredTimestamps = filteredTimestamps.filter(functions.col("timestamp-true").leq(till));
        }

        return this.transformEventModelToMap(filteredTimestamps);
    }

    @Override
    public Map<String, List<EventBoth>> querySeqTableAttributes(String logname, List<String> traceIds, Set<String> eventTypes,
                                                      Timestamp from, Timestamp till, Set<String> chosen_attributes) {
        Dataset<EventModelAttributes> eventsDF = this.readSequenceTableAttributes(logname, chosen_attributes);
        //filter based on id and based on eventType
        eventsDF = eventsDF.filter(functions.col("traceId").isin(traceIds.toArray()))
                .filter(functions.col("eventName").isin(eventTypes.toArray()));
        //filter based on the timestamps and the parameters from and till
        Dataset<Row> filteredTimestamps = eventsDF
                .withColumn("timestamp-true", functions.col("timestamp").cast("timestamp"));
        if (from != null) {
            filteredTimestamps = filteredTimestamps.filter(functions.col("timestamp-true").geq(from));
        }
        if (till != null) {
            filteredTimestamps = filteredTimestamps.filter(functions.col("timestamp-true").leq(till));
        }

        return this.transformEventModelAttributesToMap(filteredTimestamps);
    }

    /**
     * Utility class that is used from querySequenceTable methods to transform the Dataset of
     * EventModel to Map<traceId, List<EventBoth>>
     *
     * @param events a dataset of augmented EventModel (can include more than the standard fields of EventModel)
     * @return a Map of the traceIds to the corresponding events
     */
    private Map<String, List<EventBoth>> transformEventModelToMap(Dataset<Row> events) {
        List<EventModel> eventsList = events
                .select("traceId", "eventName", "timestamp", "position")
                .as(Encoders.bean(EventModel.class))
                .collectAsList();

        Map<String, List<EventBoth>> response = eventsList.parallelStream()
                .map(x ->
                        new EventBoth(x.getEventName(), x.getTraceId(), Timestamp.valueOf(x.getTimestamp()), x.getPosition()))
                .collect(Collectors.groupingByConcurrent(EventBoth::getTraceID));
        return response;
    }

    private Map<String, List<EventBoth>> transformEventModelAttributesToMap(Dataset<Row> events) {
        List<EventModelAttributes> eventsList = events
                .select("traceId", "eventName", "timestamp", "position", "attributes")
                .as(Encoders.bean(EventModelAttributes.class))
                .collectAsList();

        Map<String, List<EventBoth>> response = eventsList.parallelStream()
                .map(x ->
                        new EventBoth(x.getEventName(), x.getTraceId(), Timestamp.valueOf(x.getTimestamp()), x.getPosition(), x.getAttributes()))
                .collect(Collectors.groupingByConcurrent(EventBoth::getTraceID));
        return response;
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

    /**
     * Transform the Dataset of IndexPair (which is a utility class in storage package) to IndexRecords
     * which are objects handled by the remaining program
     *
     * @param indexPairs records from the IndexTable
     * @return an IndexRecords object
     */
    private IndexRecords transformToIndexRecords(Dataset<IndexPair> indexPairs) {
        Dataset<Row> groupedDf = indexPairs.withColumn("indexPair",
                        functions.struct("trace_id", "eventA", "eventB", "timestampA",
                                "timestampB", "positionA", "positionB", "attributesA", "attributesB"))
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
                                            eventRow.getInt(6),
                                            eventRow.getJavaMap(7),
                                            eventRow.getJavaMap(8)
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
        Dataset<EventModelAttributes> eventsDf = this.getEventsFromIndexRecordsAttributes(filteredDf);

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
            eventsDf = rows.select("eventName", "traceId", "position", "timestamp", "attributes")
                    .as(Encoders.bean(EventModelAttributes.class));
        }

        Dataset<Row> groupedDf = eventsDf
                .withColumn("event", functions.struct("traceId", "eventName", "position", "timestamp", "attributes"))
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
                                                    eventRow.getInt(2),    // position
                                                    eventRow.getJavaMap(4) // attributes
                                            );
                                        } else {
                                            return new EventTs(
                                                    eventRow.getString(1), // eventName
                                                    eventRow.getString(0), // traceId
                                                    Timestamp.valueOf(eventRow.getString(3)), // timestamp
                                                    eventRow.getJavaMap(4) // attributes
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
     * Extract events from the SingleTable and groups them based on the traceID
     *
     * @param logname    the log database
     * @param eventTypes the events that will we retrieved
     * @return
     */
    @Override
    public Map<String, List<EventBoth>> querySingleTable(String logname, Set<String> eventTypes) {
        Dataset<Trace> events = this.readSingleTable(logname)
                .filter(functions.col("eventName").isin(eventTypes.toArray()))
                .withColumn("event", functions.struct("traceId", "eventName", "timestamp", "position"))
                .groupBy("traceId")
                .agg(functions.collect_list("event").alias("events"))
                .as(Encoders.bean(Trace.class));

        Map<String, List<EventBoth>> response = events.collectAsList().stream()
                .collect(Collectors.toMap(
                                Trace::getTraceId,
                                trace -> trace.getEvents().stream().map(event -> new EventBoth(
                                        event.getEventName(),
                                        event.getTraceId(),
                                        Timestamp.valueOf(event.getTimestamp()),
                                        event.getPosition())).collect(Collectors.toList())
                        )
                );
        return response;
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
        Dataset<EventModel> data = this.readSingleTable(logname)
                .filter(functions.col("eventName").isin(eventTypes.toArray()))//filter based on type
                .filter(functions.col("traceId").isin(traceIds.toArray()));//filter based on trace
        List<EventBoth> response = data.collectAsList()
                .parallelStream().map(x -> new EventBoth(x.getEventName(),
                        x.getTraceId(), Timestamp.valueOf(x.getTimestamp()), x.getPosition()))
                .toList();
        return response;
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
    public Map<Integer, List<EventBoth>> querySingleTableGroups(String logname, List<Set<String>> groups,
                                                                Set<String> eventTypes) {
        // extract all traces that appear in groups
        Set<String> allTraces = groups.stream()
                .flatMap((java.util.function.Function<Set<String>, Stream<String>>) Collection::stream)
                .collect(Collectors.toSet());
        //create dataframe with groups
        List<Row> groupRows = new ArrayList<>();
        for (int i = 0; i < groups.size(); i++) {
            groupRows.add(RowFactory.create(i + 1, new ArrayList<>(groups.get(i)))); // (group index, traceIds)
        }
        Dataset<Row> groupDF = sparkSession.createDataFrame(groupRows, new StructType()
                .add("group_id", DataTypes.IntegerType)
                .add("traceIds", DataTypes.createArrayType(DataTypes.StringType)));


        //extract events from the single table
        Dataset<EventModel> data = this.readSingleTable(logname)
                .filter(functions.col("eventName").isin(eventTypes.toArray()))//filter based on type
                .filter(functions.col("traceId").isin(allTraces.toArray()));//filter based on trace
        // Assign group IDs to events
        Dataset<Row> eventsWithGroup = data.crossJoin(groupDF)
                .filter(functions.array_contains(functions.col("traceIds"), functions.col("traceId")))
                .select("group_id", "traceId", "eventName", "position", "timestamp"); // Keep relevant fields


        // Group by group_id and collect event details
        Dataset<Row> groupedEvents = eventsWithGroup.groupBy("group_id")
                .agg(functions.collect_list(functions.struct("eventName", "traceId", "position", "timestamp"))
                        .alias("events"));
        // Maintains only the groups that contain all the required event types
        Dataset<Row> validGroups = groupedEvents
                .withColumn("eventTypes", functions.expr("transform(events, x -> x.eventName)")) // Extract event names
                .filter(functions.size(functions.array_distinct(functions.col("eventTypes"))).equalTo(eventTypes.size()));

        //Extract from the dataframe the response that follows the format Map<group_id,List of events>
        List<GroupEvents> eventsList = validGroups
                .as(Encoders.bean(GroupEvents.class))
                .collectAsList();
        Map<Integer, List<EventBoth>> response = eventsList.parallelStream()
                .collect(Collectors.toMap(
                        GroupEvents::getGroup_id,
                        group -> group.getEvents().stream()
                                .map(event -> new EventBoth(
                                        event.getEventName(),
                                        event.getTraceId(),
                                        Timestamp.valueOf(event.getTimestamp()),
                                        event.getPosition()
                                ))
                                .sorted()
                                .collect(Collectors.toList())
                ));

        return response;
    }

    /**
     * For a given log database, returns all the event pairs found in the log
     *
     * @param logname the log database
     * @return all event pairs found in the log
     */
    @Override
    public List<Count> getEventPairs(String logname) {
        List<Count> counts = readCountTable(logname)
                .collectAsList();
        return counts;
    }

    /**
     * @param logname the log database
     * @return a list with all the event types stored in it
     */
    @Override
    public List<String> getEventNames(String logname) {
        Dataset<String> events = readSingleTable(logname)
                .select("eventName")
                .distinct()
                .as(Encoders.STRING());
        return events.collectAsList();
    }


    /**
     * Retrieves the corresponding stats (min, max duration and so on) from the CountTable, for a given set of event
     * pairs
     *
     * @param logname the log database
     * @param pairs   a set with the event pairs
     * @return a list of the stats for the set of event pairs
     */
    @Override
    public List<Count> getCounts(String logname, Set<EventPair> pairs) {
        String firstFilter = pairs.stream().map(x -> x.getEventA().getName()).collect(Collectors.toSet())
                .stream().map(x -> String.format("eventA = '%s'", x))
                .collect(Collectors.joining(" or "));
        String secondFilter = pairs.stream().map(x -> new Tuple2<>(x.getEventA().getName(), x.getEventB().getName()))
                .collect(Collectors.toSet())
                .stream().map(x -> String.format("(eventA = '%s' and eventB = '%s')", x._1(), x._2()))
                .collect(Collectors.joining(" or "));
        Dataset<Count> counts = readCountTable(logname);
        //Spark should be able to run this query efficiently and push the first filter before explosion
        List<Count> countList = counts
                .filter(firstFilter) //filter only based on the first event
                .filter(secondFilter) //filter based on the et-pair
                .collectAsList();
        return countList;
    }

    /**
     * For a given event type inside a log database, returns all the possible next events. That is, since Count
     * contains for each pair the stats, return all the events that have at least one pair with the given event
     *
     * @param logname the log database
     * @param event   the event type
     * @return the possible next events
     */
    @Override
    public List<Count> getCountForExploration(String logname, String event) {
        Dataset<Count> counts = readCountTable(logname);
        List<Count> countList = counts
                .filter(String.format("eventA = '%s'", event))
                .collectAsList();
        return countList;
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
    protected Dataset<IndexPair> transformToIndexPairSet(Dataset<Row> indexRows) {
        StructType schema = indexRows.schema();
        // Check if each column exists before selecting it
        boolean hasTimestampA = Arrays.asList(schema.fieldNames()).contains("timestampA");
        boolean hasTimestampB = Arrays.asList(schema.fieldNames()).contains("timestampB");
        boolean hasPositionA = Arrays.asList(schema.fieldNames()).contains("positionA");
        boolean hasPositionB = Arrays.asList(schema.fieldNames()).contains("positionB");

        Column traceId = functions.col("trace_id");
        Column eventA = functions.col("eventA");
        Column eventB = functions.col("eventB");

        Set<String> excluded_columns = Set.of("trace_id", "eventA", "eventB", "timestampA", "timestampB", "positionA", "positionB");
        Set<String> attribute_columns = new HashSet<>(Set.of(indexRows.columns()));
        attribute_columns.removeAll(excluded_columns);


        List<Column> mapColumnsA = new ArrayList<>();
        List<Column> mapColumnsB = new ArrayList<>();

        for (String attribute : attribute_columns) {
            if (attribute.contains("_A")) {
                mapColumnsA.add(functions.lit(attribute));
                mapColumnsA.add(functions.col(attribute));
            }
            if (attribute.contains("_B")) {
                mapColumnsB.add(functions.lit(attribute));
                mapColumnsB.add(functions.col(attribute));
            }
        }

        Column attributesA = functions.map(mapColumnsA.toArray(new Column[0]));
        Column attributesB = functions.map(mapColumnsB.toArray(new Column[0]));

        Dataset<Row> attributesDF = indexRows.withColumn("attributesA", attributesA).withColumn("attributesB", attributesB);

        attributesA = functions.col("attributesA");
        attributesB = functions.col("attributesB");

        //here is the filtering for the till and from if the indexing has been done using timestamp
        if (hasTimestampA && hasTimestampB) {
        }
        Column timestampA = hasTimestampA ? functions.col("timestampA") : functions.lit(null).cast("string");
        Column timestampB = hasTimestampB ? functions.col("timestampB") : functions.lit(null).cast("string");
        Column positionA = hasPositionA ? functions.col("positionA") : functions.lit(null).cast("int");
        Column positionB = hasPositionB ? functions.col("positionB") : functions.lit(null).cast("int");

        Dataset<IndexPair> indexPairDataset = attributesDF.select(traceId, eventA, eventB, timestampA.alias("timestampA"),
                        timestampB.alias("timestampB"), positionA.alias("positionA"),
                        positionB.alias("positionB"), attributesA, attributesB)
                .as(Encoders.bean(IndexPair.class));
        return indexPairDataset;
    }

    /**
     * Extract all events that appear in IndexTable records. Essentially, split event pairs into two
     *
     * @param indexPairs records from IndexTable in the form of a Dataset
     * @return a dataset of the unique events (i.e, uses distinct since one event might appear in multiple pairs)
     */
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

    private Dataset<EventModelAttributes> getEventsFromIndexRecordsAttributes(Dataset<IndexPair> indexPairs) {
        Column castedAttributesA = col("attributesA").cast(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
        Column castedAttributesB = col("attributesB").cast(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

        Dataset<Row> eventA_DF = indexPairs.select(
                col("eventA").alias("eventName"),
                col("timestampA").alias("timestamp"),
                col("positionA").alias("position"),
                col("trace_id").alias("traceId"),
                castedAttributesA.alias("attributes")
        );

        Dataset<Row> eventB_DF = indexPairs.select(
                col("eventB").alias("eventName"),
                col("timestampB").alias("timestamp"),
                col("positionB").alias("position"),
                col("trace_id").alias("traceId"),
                castedAttributesB.alias("attributes")
        );
        Dataset<EventModelAttributes> eventsDF = eventA_DF.union(eventB_DF)
                .dropDuplicates("eventName", "timestamp", "position", "traceId")
                .as(Encoders.bean(EventModelAttributes.class));
        return eventsDF;
    }


    //Below are for Declare//
    @Override
    public Dataset<Trace> querySequenceTableDeclare(String logname) {
        Dataset<EventModel> eventDF = this.readSequenceTable(logname);
        Dataset<Trace> groupedDF = eventDF
                .groupBy("traceId")// Group by trace_id and collect events into a list
                .agg(functions.collect_list(functions
                                .struct("eventName", "traceID", "timestamp", "position"))
                        .alias("events"))
                .as(Encoders.bean(Trace.class));
        return groupedDF;
    }

    @Override
    public Dataset<Trace> querySequenceTableDeclareAttributes(String logname, Set<String> chosen_attributes) {
        Dataset<EventModelAttributes> eventDF = this.readSequenceTableAttributes(logname, chosen_attributes);
        Dataset<Trace> groupedDF = eventDF
                .groupBy("traceId")// Group by trace_id and collect events into a list
                .agg(functions.collect_list(functions
                                .struct("eventName", "traceID", "timestamp", "position", "attributes"))
                        .alias("events"))
                .as(Encoders.bean(Trace.class));
        return groupedDF;
    }

    @Override
    public Dataset<UniqueTracesPerEventType> querySingleTableDeclare(String logname) {
        Dataset<EventModel> eventDF = this.readSingleTable(logname);
        Dataset<UniqueTracesPerEventType> uniqueTracesPerEventTypeDataset = eventDF
                .selectExpr("eventName", "traceId")
                .groupBy("eventName", "traceId")
                .agg(functions.count("*").alias("occs"))
                .withColumn("occs", functions.col("occs").cast("int"))
                .withColumn("occurrence", functions.struct("traceId", "occs"))
                .groupBy("eventName")
                .agg(functions.collect_list("occurrence").alias("occurrences"))
                .as(Encoders.bean(UniqueTracesPerEventType.class));
        return uniqueTracesPerEventTypeDataset;
    }

    @Override
    public Dataset<EventSupport> querySingleTable(String logname) {
        Dataset<EventModel> eventDF = this.readSingleTable(logname);
        Dataset<EventSupport> supportDF = eventDF.select("eventName", "traceId")
                .groupBy("eventName")
                .agg(functions.count("traceId").alias("support"))
                .selectExpr("eventName as event", "support")
                .as(Encoders.bean(EventSupport.class));
        return supportDF;
    }

    @Override
    public Dataset<UniqueTracesPerEventPair> queryIndexTableDeclare(String logname) {
        Dataset<IndexPair> indexRecords = readIndexTable(logname);

        Dataset<UniqueTracesPerEventPair> uniqueTracesPerEventPairDataset = indexRecords
                .select("eventA", "eventB", "trace_id")
                .distinct()
                .groupBy("eventA", "eventB")
                .agg(functions.collect_list("trace_id").alias("uniqueTraces"))
                .as(Encoders.bean(UniqueTracesPerEventPair.class));

        return uniqueTracesPerEventPairDataset;
    }

    @Override
    public Dataset<EventPairToTrace> queryIndexOriginalDeclare(String logname) {
        Dataset<IndexPair> indexPairDataset = readIndexTable(logname);
        Dataset<EventPairToTrace> response = indexPairDataset
                .select("eventA", "eventB", "trace_id")
                .distinct()
                .as(Encoders.bean(EventPairToTrace.class));
        return response;
    }

    @Override
    public Dataset<EventTypeTracePositions> querySingleTableAllDeclare(String logname) {
        Dataset<EventModel> eventDF = this.readSingleTable(logname);
        Dataset<EventTypeTracePositions> response = eventDF
                .select("eventName","traceId","position")
                .groupBy("eventName","traceId")
                .agg(functions.collect_list("position").alias("positions"))
                .as(Encoders.bean(EventTypeTracePositions.class));
        return response;
    }

}
