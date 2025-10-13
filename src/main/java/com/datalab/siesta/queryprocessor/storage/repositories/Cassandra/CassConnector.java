package com.datalab.siesta.queryprocessor.storage.repositories.Cassandra;

import com.datalab.siesta.queryprocessor.declare.model.declareState.*;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.model.EventModel;
import com.datalab.siesta.queryprocessor.storage.model.Trace;
import com.datalab.siesta.queryprocessor.storage.repositories.SparkDatabaseRepository;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import scala.Tuple2;
import scala.collection.JavaConverters;


import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@Configuration
@PropertySource("classpath:application.properties")
@ConditionalOnExpression("'${database}' == 'cassandra'")
public class CassConnector extends SparkDatabaseRepository {


    protected String bucket = "s3a://siesta/";

    @Autowired
    public CassConnector(SparkSession sparkSession, JavaSparkContext javaSparkContext, Utils utils) {
        super(sparkSession, javaSparkContext, utils);
    }

    @Override
    public Metadata getMetadata(String logname) {
        Dataset<Row> df = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", logname + "_meta", "keyspace", "siesta"))
                .load();
        try {
            Map<String, String> m = new HashMap<>();
            df.toJavaRDD().map((Function<Row, Tuple2<String, String>>) row ->
                            new Tuple2<>(row.getString(0), row.getString(1)))
                    .collect().forEach(t -> {
                        m.put(t._1, t._2);
                    });
            return new Metadata(m);

        } catch (Exception e){ //handle metadata from delta
            Map<String, String> metadataMap = new HashMap<>();
            List<Row> rows = df.collectAsList(); // Collect rows as a list
            for (Row row : rows) {
                String key = row.getAs("key");
                String value = row.getAs("value");
                metadataMap.put(key, value);
            }
            return new Metadata(metadataMap, "delta");
        }
    }

    @Override
    public Set<String> findAllLongNames() {
        CassandraConnector connector = CassandraConnector.apply(sparkSession.sparkContext().getConf());
        List<String> keywords = new ArrayList<>() {{
            add("set");
            add("sign");
            add("meta");
            add("idx");
            add("count");
            add("index");
            add("seq");
            add("lastchecked");
            add("single");
        }};
        return connector.withSessionDo(session -> session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '"
                        + "siesta" + "';").all())
                .stream().map(x -> x.get("table_name", String.class)).filter(Objects::nonNull)
                .map(x ->
                        Arrays.stream(x.split("_")).
                                filter(y -> !keywords.contains(y)).collect(Collectors.joining("_"))
                ).collect(Collectors.toSet());
    }

    @Override
    protected Dataset<EventModel> readSequenceTable(String logname) {
        String path = String.format("%s_seq", logname);
        Dataset<Row> df = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", path, "keyspace", "siesta"))
                .load();

        // Explode the sequence data and transform to EventModel format
        Dataset<Row> explodedDF = df
                .withColumn("event_data", functions.explode(functions.col("events")))
                .withColumn("event_parts", functions.split(functions.col("event_data"), ","))
                .withColumn("timestamp", functions.element_at(functions.col("event_parts"), 1))
                .withColumn("event_name", functions.element_at(functions.col("event_parts"), 2))
                .withColumn("position", functions.row_number().over(
                        Window.partitionBy("sequence_id").orderBy(functions.monotonically_increasing_id())
                ).minus(1))
                .select(
                        functions.col("sequence_id").alias("traceId"),
                        functions.col("event_name").alias("eventName"),
                        functions.col("timestamp"),
                        functions.col("position")
                );
        return explodedDF.as(Encoders.bean(EventModel.class));
    }

    @Override
    protected Dataset<EventModel> readSingleTable(String logname){
        String tableName = String.format("%s_single", logname);
        Dataset<Row> df = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", tableName, "keyspace", "siesta"))
                .load();

        // Explode the occurrences list and parse each occurrence
        Dataset<Row> explodedDF = df
                .withColumn("occurrence", functions.explode(functions.col("occurrences")))
                .withColumn("occurrence_parts", functions.split(functions.col("occurrence"), ","))
                .withColumn("timestamp", functions.element_at(functions.col("occurrence_parts"), 1))
                .withColumn("position", functions.element_at(functions.col("occurrence_parts"), 2).cast("int"))
                .select(
                        functions.col("trace_id").alias("traceId"),
                        functions.col("event_type").alias("eventName"),
                        functions.col("timestamp"),
                        functions.col("position")
                );

        return explodedDF.as(Encoders.bean(EventModel.class));
    }

    @Override
    protected Dataset<Count> readCountTable(String logname){
        String tableName = String.format("%s_count", logname);
        Dataset<Row> df = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", tableName, "keyspace", "siesta"))
                .load();

        // Explode the times list and parse each time record
        Dataset<Row> explodedDF = df
                .withColumn("time_record", functions.explode(functions.col("times")))
                .withColumn("time_parts", functions.split(functions.col("time_record"), ","))
                .select(
                        functions.col("event_a").alias("eventA"),
                        functions.element_at(functions.col("time_parts"), 1).alias("eventB"),
                        functions.element_at(functions.col("time_parts"), 2).cast("long").alias("sumDuration"),
                        functions.element_at(functions.col("time_parts"), 3).cast("int").alias("count"),
                        functions.element_at(functions.col("time_parts"), 4).cast("long").alias("minDuration"),
                        functions.element_at(functions.col("time_parts"), 5).cast("long").alias("maxDuration")
                        ,functions.element_at(functions.col("time_parts"), 6).cast("long").alias("sumSquares")
                );
        explodedDF.show();
        return explodedDF.as(Encoders.bean(Count.class));
    }

    protected Dataset<IndexPair> readIndexTable(String logname) {
        String tableName = String.format("%s_index", logname);
        Dataset<Row> df = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(Map.of("table", tableName, "keyspace", "siesta"))
                .load();

        // Explode the occurrences list and parse each occurrence
        Dataset<Row> explodedDF = df
                .withColumn("occurrence", functions.explode(functions.col("occurrences")))
                .withColumn("occurrence_parts", functions.split(functions.col("occurrence"), ","))
                .withColumn("trace_id", functions.element_at(functions.col("occurrence_parts"), 1))
                .withColumn("positionA", functions.element_at(functions.col("occurrence_parts"), 2).cast("int"))
                .withColumn("positionB", functions.element_at(functions.col("occurrence_parts"), 3).cast("int"))
                .select(
                        functions.col("trace_id"),
                        functions.col("event_a").alias("eventA"),
                        functions.col("event_b").alias("eventB"),
                        functions.col("start").cast("string").alias("timestampA"),
                        functions.col("end").cast("string").alias("timestampB"),
                        functions.col("positionA"),
                        functions.col("positionB")
                );

        return explodedDF.as(Encoders.bean(IndexPair.class));
    }

    //Below are for declare//
    @Override
    public Dataset<PositionState> queryPositionState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/position.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(PositionState.class));
    }

    @Override
    public Dataset<ExistenceState> queryExistenceState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/existence.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(ExistenceState.class));
    }


    @Override
    public Dataset<UnorderStateI> queryUnorderStateI(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/unorder/i.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(UnorderStateI.class));
    }


    @Override
    public Dataset<UnorderStateU> queryUnorderStateU(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/unorder/u.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(UnorderStateU.class));
    }


    @Override
    public Dataset<OrderState> queryOrderState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/order.parquet");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(OrderState.class));
    }


    @Override
    public Dataset<NegativeState> queryNegativeState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/negatives.parquet");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(NegativeState.class));
    }


}
