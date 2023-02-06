package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraintWE;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraintWE;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexMiddleResult;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.EventPair;
import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.storage.repositories.SparkDatabaseRepository;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.collection.Iterable;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnProperty(
        value = "database",
        havingValue = "s3",
        matchIfMissing = true
)
@Service
public class S3Connector extends SparkDatabaseRepository {

    protected SparkSession sparkSession;

    protected JavaSparkContext javaSparkContext;

    private String bucket = "s3a://siesta/";

    @Autowired
    public S3Connector(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        this.sparkSession = sparkSession;
        this.javaSparkContext = javaSparkContext;
    }


    @Override
    public Metadata getMetadata(String logname) {
        Dataset<Row> df = sparkSession.read().json(String.format("%s%s%s", bucket, logname, "/meta.parquet/"));
        return new Metadata(df.collectAsList().get(0));
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
    public List<Count> getCounts(String logname, Set<EventPair> pairs) {
        String path = String.format("%s%s%s", bucket, logname, "/count.parquet/");
        String firstFilter = pairs.stream().map(x -> x.getEventA().getName()).collect(Collectors.toSet())
                .stream().map(x -> String.format("eventA = '%s'", x)).collect(Collectors.joining(" or "));
        Broadcast<Set<EventPair>> b_pairs = javaSparkContext.broadcast(pairs);
        List<Count> counts = sparkSession.read()
                .parquet(path)
                .where(firstFilter)
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, Count>) row -> {
                    String eventA = row.getString(1);
                    List<Row> countRecords = JavaConverters.seqAsJavaList(row.getSeq(0));
                    List<Count> c = new ArrayList<>();
                    for (Row v1 : countRecords) {
                        String eventB = v1.getString(0);
                        long sum_duration = v1.getLong(1);
                        int count = v1.getInt(2);
                        long min_duration = v1.getLong(3);
                        long max_daration = v1.getLong(4);
                        c.add(new Count(eventA, eventB, sum_duration, count, min_duration, max_daration));
                    }
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
    public List<String> getEventNames(String logname) {
        String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");
        return sparkSession.read().parquet(path)
                .select("event_type")
                .toJavaRDD()
                .map((Function<Row, String>) row -> row.getString(0))
                .collect();
    }

    @Override
    public IndexMiddleResult patterDetectionTraceIds(String logname, List<Tuple2<EventPair, Count>> combined, Metadata metadata) {
        Set<EventPair> pairs = combined.stream().map(x -> x._1).collect(Collectors.toSet());
        JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> gpairs =this.getAllEventPairs(pairs, logname, metadata);
        Tuple2<List<TimeConstraintWE>, List<GapConstraintWE>> x = this.splitConstraints(pairs);
        return null;
    }



    @Override
    protected JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs, String logname, Metadata metadata) {
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");
        String filterEvents = pairs.stream().map(x ->
                String.format(" (eventA='%s' and eventB='%s') ", x.getEventA().getName(), x.getEventB().getName())
        ).collect(Collectors.joining("or"));
        Broadcast<String> mode = javaSparkContext.broadcast(metadata.getMode());
        JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> df = sparkSession.read()
                .parquet(path)
                .where(filterEvents)
                .toJavaRDD()
                .flatMap((FlatMapFunction<Row, IndexPair>) row -> {
                    String eventA = row.getAs("eventA");
                    String eventB = row.getAs("eventB");
                    List<Row> l = JavaConverters.seqAsJavaList(row.getSeq(1));
                    List<IndexPair> response = new ArrayList<>();
                    for (Row r2 : l) {
                        long tid = r2.getLong(0);
                        if (mode.getValue().equals("positions")) {
                            List<Row> inner = JavaConverters.seqAsJavaList(r2.getSeq(1));
                            int posA = inner.get(0).getInt(0);
                            int posB = inner.get(0).getInt(1);
                            response.add(new IndexPair(tid, eventA, eventB, posA, posB));
                        } else {
                            List<Row> inner = JavaConverters.seqAsJavaList(r2.getSeq(1));
                            String tsA = inner.get(0).getString(0);
                            String tsB = inner.get(0).getString(1);
                            response.add(new IndexPair(tid, eventA, eventB, tsA, tsB));
                        }
                    }
                    return response.iterator();
                })
                .groupBy((Function<IndexPair, Tuple2<String,String>>) indexPair-> new Tuple2<>(indexPair.getEventA(),indexPair.getEventB()));
        return df;
    }
}