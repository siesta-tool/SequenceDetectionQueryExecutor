package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.declare.model.EventPairToTrace;
import com.datalab.siesta.queryprocessor.declare.model.EventSupport;
import com.datalab.siesta.queryprocessor.declare.model.OccurrencesPerTrace;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventPair;
import com.datalab.siesta.queryprocessor.declare.model.UniqueTracesPerEventType;
import com.datalab.siesta.queryprocessor.declare.model.declareState.ExistenceState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.NegativeState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.OrderState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.PositionState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateI;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateU;
import com.datalab.siesta.queryprocessor.model.DBModel.*;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.model.EventModel;
import com.datalab.siesta.queryprocessor.storage.model.Trace;
import com.datalab.siesta.queryprocessor.storage.repositories.SparkDatabaseRepository;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Configuration
//@ConditionalOnProperty(
//        value = "database",
//        havingValue = "s3",
//        matchIfMissing = true
//)
@ConditionalOnExpression("'${database}' == 's3' and '${delta}' == 'false'")
@Service
public class S3Connector extends SparkDatabaseRepository {


    private String bucket = "s3a://siesta/";

    @Autowired
    public S3Connector(SparkSession sparkSession, JavaSparkContext javaSparkContext, Utils utils) {
        super(sparkSession, javaSparkContext, utils);
    }


    @Override
    public Metadata getMetadata(String logname) {
        Dataset<Row> df = sparkSession.read().parquet(String.format("%s%s%s", bucket, logname, "/meta.parquet/"));
        return new Metadata(df.toJavaRDD().collect().get(0));
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
        String path = String.format("%s%s%s", bucket, logname, "/seq.parquet/");
        Dataset<EventModel> eventsDF = sparkSession.read().parquet(path)
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
        String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");
        Dataset<EventModel> eventsDF = sparkSession.read().parquet(path)
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
        String path = String.format("%s%s%s", bucket, logname, "/count.parquet/");
        Dataset<Row> df = sparkSession.read().parquet(path);
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
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");
        Dataset<Row> indexRecords = sparkSession.read()
                .parquet(path);
        Dataset<IndexPair> fixMissingFields = super.transformToIndexPairSet(indexRecords)
                .as(Encoders.bean(IndexPair.class));
        return fixMissingFields;
    }


    //Below are for declare//







    @Override
    public JavaRDD<IndexPair> queryIndexTableAllDeclare(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");

        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .map((Function<Row, IndexPair>) row -> {
                    String eventA = row.getAs("eventA");
                    String eventB = row.getAs("eventB");
                    String trace_id = row.getAs("trace_id");
                    int positionA = row.getAs("positionA");
                    int positionB = row.getAs("positionB");
                    return new IndexPair(trace_id,eventA,eventB,positionA,positionB);
                });
    }


    @Override
    public JavaRDD<PositionState> queryPositionState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/position.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(PositionState.class))
        .toJavaRDD();
    }

    @Override
    public JavaRDD<ExistenceState> queryExistenceState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/existence.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(ExistenceState.class))
        .toJavaRDD();
    }


    @Override
    public JavaRDD<UnorderStateI> queryUnorderStateI(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/unorder/i.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(UnorderStateI.class))
        .toJavaRDD();
    }


    @Override
    public JavaRDD<UnorderStateU> queryUnorderStateU(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/unorder/u.parquet/");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(UnorderStateU.class))
        .toJavaRDD();
    }


    @Override
    public JavaRDD<OrderState> queryOrderState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/order.parquet");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(OrderState.class))
        .toJavaRDD();
    }


    @Override
    public JavaRDD<NegativeState> queryNegativeState(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/declare/negatives.parquet");

        return sparkSession.read()
        .parquet(path)
        .as(Encoders.bean(NegativeState.class))
        .toJavaRDD();
    }


}
