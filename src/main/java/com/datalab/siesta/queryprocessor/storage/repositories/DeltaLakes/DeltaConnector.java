package com.datalab.siesta.queryprocessor.storage.repositories.DeltaLakes;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.model.EventModel;
import com.datalab.siesta.queryprocessor.storage.repositories.S3.S3Connector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Configuration
@PropertySource("classpath:application.properties")
@ConditionalOnExpression("'${database}' == 's3' and '${delta}' == 'true'")
public class DeltaConnector extends S3Connector {

    @Autowired
    public DeltaConnector(SparkSession sparkSession, JavaSparkContext javaSparkContext, Utils utils) {
        super(sparkSession, javaSparkContext, utils);
    }


    @Override
    public Metadata getMetadata(String logname) {
        String path = String.format(String.format("%s%s%s", bucket, logname, "/meta"));
        Dataset<Row> df = sparkSession.read().format("delta").load(path);
        Map<String, String> metadataMap = new HashMap<>();
        List<Row> rows = df.collectAsList(); // Collect rows as a list
        for (Row row : rows) {
            String key = row.getAs("key");
            String value = row.getAs("value");
            metadataMap.put(key, value);
        }
        return new Metadata(metadataMap, "delta");
    }

    @Override
    public Set<String> findAllLongNames() {
        try {
            FileSystem fs = FileSystem.get(new URI(this.bucket), sparkSession.sparkContext().hadoopConfiguration());
            RemoteIterator<LocatedFileStatus> f = fs.listFiles(new Path(this.bucket), true);
            Pattern pattern = Pattern.compile(String.format("%s[^/]*/", this.bucket));
            Set<String> files = new HashSet<>();
            while (f.hasNext()) {
                LocatedFileStatus fin = f.next();
                Matcher matcher = pattern.matcher(fin.getPath().toString());
                if (matcher.find()) {
                    String logname = matcher.group(0).replace(this.bucket, "").replace("/", "");
                    files.add(logname);
                }
            }
            return files;

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Dataset<EventModel> readSequenceTable(String logname){
        String path = String.format("%s%s%s", bucket, logname, "/seq/");
        Dataset<EventModel> eventsDF = sparkSession.read()
                .format("delta")
                .load(path)
                .selectExpr(
                        "trace as traceId",
                        "event_type as eventName",
                        "CAST(timestamp AS STRING) as timestamp",  // Ensure timestamp is correctly formatted
                        "position"
                )
                .as(Encoders.bean(EventModel.class));
        return eventsDF;
    }

    @Override
    protected Dataset<EventModel> readSingleTable(String logname){
        String path = String.format("%s%s%s", bucket, logname, "/single/");
        Dataset<EventModel> eventsDF = sparkSession.read()
                .format("delta")
                .load(path)
                .selectExpr(
                        "trace as traceId",
                        "event_type as eventName",
                        "CAST(timestamp AS STRING) as timestamp",  // Ensure timestamp is correctly formatted
                        "position"
                )
                .as(Encoders.bean(EventModel.class));
        return eventsDF;
    }

    @Override
    protected Dataset<Count> readCountTable(String logname){
        String path = String.format("%s%s%s", bucket, logname, "/count/");
        Dataset<Row> df = sparkSession.read()
                .format("delta")
                .load(path);
        Dataset<Row> explodedDf = df
                .withColumn("countRecord", functions.explode(
                        df.col("times")))
                .select(
                        df.col("eventA"),
                        functions.col("countRecord._1").alias("eventB"),
                        functions.col("countRecord._2").alias("sumDuration"),
                        functions.col("countRecord._3").alias("count"),
                        functions.col("countRecord._4").alias("minDuration"),
                        functions.col("countRecord._5").alias("maxDuration"),
                        functions.col("countRecord._6").alias("sumSquares")
                );
        Dataset<Count> counts = explodedDf.as(Encoders.bean(Count.class));
        return counts;
    }

    protected Dataset<IndexPair> readIndexTable(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/index/");
        Dataset<Row> indexRecords = sparkSession.read()
                .format("delta")
                .load(path);
        Dataset<IndexPair> fixMissingFields = super.transformToIndexPairSet(indexRecords)
                .as(Encoders.bean(IndexPair.class));
        return fixMissingFields;
    }



    @Override
    public List<Count> getCountForExploration(String logname, String event) {
        String path = String.format("%s%s%s", bucket, logname, "/count/");
        List<Count> counts = sparkSession.read()
                .format("delta")
                .load(path)
                .where(String.format("eventA = '%s'", event))
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, Count>) row -> {
                    List<Count> c = new ArrayList<>();
                    String eventA = row.getString(0);
                    String eventB = row.getString(1);
                    long sum_duration = row.getLong(2);
                    int count = row.getInt(3);
                    long min_duration = row.getLong(4);
                    long max_duration = row.getLong(5);
                    double sum_squared = row.getDouble(6);
                    c.add(new Count(eventA, eventB, sum_duration, count, min_duration, max_duration, sum_squared));
                    return c.iterator();
                }).collect();
        return new ArrayList<>(counts);
    }

    @Override
    public List<Count> getCounts(String logname, Set<EventPair> pairs) {
        String path = String.format("%s%s%s", bucket, logname, "/count/");
        String firstFilter = pairs.stream().map(x -> x.getEventA().getName()).collect(Collectors.toSet())
                .stream().map(x -> String.format("eventA = '%s'", x)).collect(Collectors.joining(" or "));
        Broadcast<Set<EventPair>> b_pairs = javaSparkContext.broadcast(pairs);

        Dataset<Row> df = sparkSession.read()
                .format("delta")
                .load(path)
                .where(firstFilter);

        // Print the schema of the DataFrame
        System.out.println("Schema of the DataFrame:");
        df.printSchema();
        System.out.println("Seires: " + df.count());
        System.out.println("Sthles: " + df.columns().length);
        List<Count> counts = sparkSession.read()
                .format("delta")
                .load(path)
                .where(firstFilter)
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, Count>) row -> {
                    List<Count> c = new ArrayList<>();
                    String eventA = row.getString(0);
                    String eventB = row.getString(1);
                    long sum_duration = row.getLong(2);
                    int count = row.getInt(3);
                    long min_duration = row.getLong(4);
                    long max_duration = row.getLong(5);
                    double sum_squared = row.getDouble(6);
                    c.add(new Count(eventA, eventB, sum_duration, count, min_duration, max_duration, sum_squared));
                    return c.iterator();
                })
                .filter((Function<Count, Boolean>) c -> {
                    for (EventPair p : b_pairs.getValue()) {
                        if (c.getEventA().equals(p.getEventA().getName()) && c.getEventB().equals(p.getEventB().getName())) {
                            return true;
                        }
                    }
                    return false;
                })
                .collect();
        List<Count> response = new ArrayList<>();
        pairs.forEach(p -> {
            for (Count c : counts) {
                if (c.getEventA().equals(p.getEventA().getName()) && c.getEventB().equals(p.getEventB().getName())) {
                    response.add(c);
                    break;
                }
            }
        });

        return response;
    }

    @Override
    public List<Count> getEventPairs(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/count/");
        List<Count> counts = sparkSession.read()
                .format("delta")
                .load(path)
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, Count>) row -> {
                    List<Count> c = new ArrayList<>();
                    String eventA = row.getString(0);
                    String eventB = row.getString(1);
                    long sum_duration = row.getLong(2);
                    int count = row.getInt(3);
                    long min_duration = row.getLong(4);
                    long max_duration = row.getLong(5);
                    double sum_squared = row.getDouble(6);
                    c.add(new Count(eventA, eventB, sum_duration, count, min_duration, max_duration, sum_squared));
                    return c.iterator();
                })
                .collect();
        return counts;
    }
}
