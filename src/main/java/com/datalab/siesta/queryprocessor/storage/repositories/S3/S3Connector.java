package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.declare.model.declareState.ExistenceState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.NegativeState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.OrderState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.PositionState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateI;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateU;
import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.model.EventModel;
import com.datalab.siesta.queryprocessor.storage.repositories.SparkDatabaseRepository;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Configuration
@PropertySource("classpath:application.properties")
@ConditionalOnExpression("'${database}' == 's3'")
public class S3Connector extends SparkDatabaseRepository {


    protected String bucket = "s3a://siesta/";

    @Autowired
    public S3Connector(SparkSession sparkSession, JavaSparkContext javaSparkContext, Utils utils) {
        super(sparkSession, javaSparkContext, utils);
    }


    @Override
    public Metadata getMetadata(String logname) {
        Dataset<Row> df = null;
        boolean parquet = true;
        try{
             df = sparkSession.read().parquet(String.format("%s%s%s", bucket, logname, "/meta.parquet/"));
        }catch (Exception e){
            try {
                String path = String.format(String.format("%s%s%s", bucket, logname, "/meta/"));
                df = sparkSession.read().format("delta").load(path);
                parquet = false;
            }catch (Exception e2){
                return null;
            }
        }
        if(parquet){ //handle metadata from parquets
            return new Metadata(df.toJavaRDD().collect().get(0));
        }else{ //handle metadata from delta
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
    public List<String> getAttributes(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/seq.parquet/");

        Dataset<Row> df = sparkSession.read().parquet(path).select("attributes").toDF();

        System.out.println(df);

        Dataset<Row> keysDf = df.select(functions.explode(functions.map_keys(df.col("attributes"))).alias("key"));

        return keysDf.dropDuplicates()
                .collectAsList()
                .stream()
                .map(row -> row.getString(0)) // Extract string from Row
                .collect(Collectors.toList());
    }

    @Override
    protected Dataset<EventModel> readSequenceTable(String logname){
        Dataset<Row> df;
        try{
            String path = String.format("%s%s%s", bucket, logname, "/seq.parquet/");
            df = sparkSession.read().parquet(path);
        }catch (Exception e){
            String path = String.format("%s%s%s", bucket, logname, "/seq/");
            df = sparkSession.read().format("delta").load(path)
                    .withColumnRenamed("trace","trace_id");
        }
        Dataset<EventModel> eventsDF = df
                .selectExpr(
                        "trace_id as traceId",
                        "event_type as eventName",
                        "CAST(timestamp AS STRING) as timestamp",  // Ensure timestamp is correctly formatted
                        "position"
                )
                .as(Encoders.bean(EventModel.class));
        return eventsDF;
    }

    @Override
    protected Dataset<EventModel> readSingleTable(String logname){
        Dataset<Row> df;
        try{
            String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");
            df = sparkSession.read().parquet(path);
        }catch (Exception e){
            String path = String.format("%s%s%s", bucket, logname, "/single/");
            df = sparkSession.read().format("delta").load(path)
                    .withColumnRenamed("trace","trace_id");
        }
        Dataset<EventModel> eventsDF = df
                .selectExpr(
                        "trace_id as traceId",
                        "event_type as eventName",
                        "CAST(timestamp AS STRING) as timestamp",  // Ensure timestamp is correctly formatted
                        "position"
                )
                .as(Encoders.bean(EventModel.class));
        return eventsDF;
    }

    @Override
    protected Dataset<Count> readCountTable(String logname){
        Dataset<Row> df;

        try{
            String path = String.format("%s%s%s", bucket, logname, "/count.parquet/");
            df = sparkSession.read().parquet(path)
                    .withColumn("countRecord", functions.explode(
                            functions.col("times")))
                    .select(
                            functions.col("eventA"),
                            functions.col("countRecord._1").alias("eventB"),
                            functions.col("countRecord._2").alias("sumDuration"),
                            functions.col("countRecord._3").alias("count"),
                            functions.col("countRecord._4").alias("minDuration"),
                            functions.col("countRecord._5").alias("maxDuration"),
                            functions.col("countRecord._6").alias("sumSquares")
                    );
        }catch (Exception e){
            String path = String.format("%s%s%s", bucket, logname, "/count/");
            df = sparkSession.read().format("delta").load(path)
                    .withColumnRenamed("sum_duration","sumDuration")
                    .withColumnRenamed("min_duration","minDuration")
                    .withColumnRenamed("max_duration","maxDuration")
                    .withColumnRenamed("sum_squares","sumSquares");
        }
        Dataset<Count> counts = df.as(Encoders.bean(Count.class));
        return counts;
    }

    protected Dataset<IndexPair> readIndexTable(String logname) {
        Dataset<Row> df;
        try{
            String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");
            df = sparkSession.read().parquet(path);
        }catch (Exception e){
            String path = String.format("%s%s%s", bucket, logname, "/index/");
            df = sparkSession.read().format("delta").load(path)
                    .withColumnRenamed("id","trace_id")
                    .withColumn("timestampA",functions.col("timeA").cast("string"))
                    .withColumn("timestampB",functions.col("timeB").cast("string"));
        }
        Dataset<IndexPair> fixMissingFields = super.transformToIndexPairSet(df)
                .as(Encoders.bean(IndexPair.class));
        return fixMissingFields;
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
