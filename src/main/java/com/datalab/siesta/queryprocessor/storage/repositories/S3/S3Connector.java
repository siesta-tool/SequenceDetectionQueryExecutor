package com.datalab.siesta.queryprocessor.storage.repositories.S3;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.DBModel.Trace;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
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
import scala.collection.JavaConverters;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
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


    private Utils utils;
    private String bucket = "s3a://siesta/";

    @Autowired
    public S3Connector(SparkSession sparkSession, JavaSparkContext javaSparkContext, Utils utils) {
        super(sparkSession, javaSparkContext);
        this.utils = utils;
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
    protected JavaRDD<Trace> querySequenceTablePrivate(String logname, Broadcast<Set<Long>> bTraceIds) {
        String path = String.format("%s%s%s", bucket, logname, "/seq.parquet/");
        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .map((Function<Row, Trace>) row -> {
                    long trace_id = row.getAs("trace_id");
                    List<Row> evs = JavaConverters.seqAsJavaList(row.getSeq(1));
                    List<EventBoth> results = new ArrayList<>();
                    for (int i = 0; i < evs.size(); i++) {
                        String event_name = evs.get(i).getString(0);
                        Timestamp event_timestamp = Timestamp.valueOf(evs.get(i).getString(1));
                        results.add(new EventBoth(event_name, event_timestamp, i));
                    }
                    return new Trace(trace_id, results);
                })
                .filter((Function<Trace, Boolean>) trace -> bTraceIds.getValue().contains(trace.getTraceID()));
    }


    @Override
    protected JavaRDD<EventBoth> getFromSingle(String logname, Set<Long> traceIds, Set<String> eventTypes) {
        String path = String.format("%s%s%s", bucket, logname, "/single.parquet/");
        Broadcast<Set<Long>> bTraceIds = javaSparkContext.broadcast(traceIds);
        Broadcast<Set<String>> bEventTypes = javaSparkContext.broadcast(eventTypes);
        return sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .filter((Function<Row, Boolean>) x -> bEventTypes.value().contains(x.getString(1)))
                .flatMap((FlatMapFunction<Row, EventBoth>) row -> {
                    String eventType = row.getString(1);
                    List<EventBoth> events = new ArrayList<>();
                    List<Row> occurrences = JavaConverters.seqAsJavaList(row.getSeq(0));
                    for (Row occurrence : occurrences) {
                        long traceId = occurrence.getLong(0);
                        if (bTraceIds.value().contains(traceId)) {
                            List<String> times = JavaConverters.seqAsJavaList(occurrence.getSeq(1));
                            List<Integer> positions = JavaConverters.seqAsJavaList(occurrence.getSeq(2));
                            for (int i = 0; i < times.size(); i++) {
                                events.add(new EventBoth(eventType, traceId, Timestamp.valueOf(times.get(i)), positions.get(i)));
                            }
                        }
                    }
                    return events.iterator();
                });
    }


    @Override
    protected JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> getAllEventPairs(Set<EventPair> pairs,
                                                                                                  String logname,
                                                                                                  Metadata metadata,
                                                                                                  Timestamp from,
                                                                                                  Timestamp till) {
        String path = String.format("%s%s%s", bucket, logname, "/index.parquet/");
        Broadcast<Set<EventPair>> bPairs = javaSparkContext.broadcast(pairs);
        Broadcast<String> mode = javaSparkContext.broadcast(metadata.getMode());
        Broadcast<Timestamp> bFrom = javaSparkContext.broadcast(from);
        Broadcast<Timestamp> bTill = javaSparkContext.broadcast(till);
        JavaPairRDD<Tuple2<String, String>, java.lang.Iterable<IndexPair>> df = sparkSession.read()
                .parquet(path)
                .toJavaRDD()
                .filter((Function<Row, Boolean>) row -> {
                    Timestamp start = row.getAs("start");
                    Timestamp end = row.getAs("end");
                    if (bFrom.value() != null && bFrom.value().after(end)) return false;
                    if (bTill.value() != null && bTill.value().before(start)) return false;
                    return true;
                })
                .flatMap((FlatMapFunction<Row, IndexPair>) row -> {
                    String eventA = row.getAs("eventA");
                    String eventB = row.getAs("eventB");
                    List<Row> l = JavaConverters.seqAsJavaList(row.getSeq(1));
                    List<IndexPair> response = new ArrayList<>();
                    for (Row r2 : l) {
                        long tid = r2.getLong(0);
                        List<Row> innerList = JavaConverters.seqAsJavaList(r2.getSeq(1));
                        if (mode.getValue().equals("positions")) {
                            for (Row inner : innerList) {
                                int posA = inner.getInt(0);
                                int posB = inner.getInt(1);
                                response.add(new IndexPair(tid, eventA, eventB, posA, posB));
                            }
                        } else {
                            for (Row inner : innerList) {
                                String tsA = inner.getString(0);
                                String tsB = inner.getString(1);
                                response.add(new IndexPair(tid, eventA, eventB, tsA, tsB));
                            }
                        }
                    }
                    return response.iterator();
                })
                .filter((Function<IndexPair, Boolean>) indexPairs -> indexPairs.validate(bPairs.getValue()))
                .filter((Function<IndexPair, Boolean>) p->{
                    if(mode.value().equals("timestamps")) {
                        if(bTill.value()!=null && p.getTimestampA().after(bTill.value())) return false;
                        if(bFrom.value()!=null && p.getTimestampB().before(bFrom.value())) return false;
                    }
                    //If from and till has been set we cannot check it here
                    return true;
                })
                .groupBy((Function<IndexPair, Tuple2<String, String>>) indexPair -> new Tuple2<>(indexPair.getEventA(), indexPair.getEventB()));
        return df;
    }
}
