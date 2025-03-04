package com.datalab.siesta.queryprocessor.declare.queryPlans.position;

import java.util.List;

import com.datalab.siesta.queryprocessor.declare.model.EventSupport;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePosition;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryPositionWrapper;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.storage.model.Trace;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import scala.Tuple2;

@Component
@RequestScope
public class QueryPlanPositions implements QueryPlan{

    /**
     * Connection with the database
     */
    protected final DeclareDBConnector declareDBConnector;

    /**
     * Connection with the spark 
     */
    protected JavaSparkContext javaSparkContext;

    /**
     * Log Database's metadata
     */
    protected Metadata metadata;


    @Autowired
    public QueryPlanPositions(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext){
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryPositionWrapper qpw = (QueryPositionWrapper) qw;
        //get number of total traces in database
        Long totalTraces = metadata.getTraces();
        //get all sequences from the sequence table
        Dataset<Trace> traces = declareDBConnector.querySequenceTableDeclare(this.metadata.getLogname());

        Dataset<Row> firstLast = traces.withColumn("firstEvent", functions.expr("events[0].eventName")) // First event name
                .withColumn("lastEvent", functions.expr("events[size(events) - 1].eventName")) // Last event name
                .select("firstEvent", "lastEvent");
        firstLast.persist(StorageLevel.MEMORY_AND_DISK());

        List<EventSupport> first = firstLast
                .groupBy("firstEvent")
                .agg(functions.count("*").alias("count"))
                .selectExpr("firstEvent as event", String.format("count/%s as support",totalTraces))
                .filter(functions.col("support").geq((qpw.getSupport())))
                .as(Encoders.bean(EventSupport.class))
                .collectAsList();

        List<EventSupport> last = firstLast
                .groupBy("lastEvent")
                .agg(functions.count("*").alias("count"))
                .selectExpr("lastEvent as event", String.format("count/%s as support",totalTraces))
                .filter(functions.col("support").geq((qpw.getSupport())))
                .as(Encoders.bean(EventSupport.class))
                .collectAsList();


        //set up the response
        QueryResponsePosition response = new QueryResponsePosition();
        if(qpw.getMode().equals("first")){
            response.setFirst(first);
        }else if(qpw.getMode().equals("last")){
            response.setLast(last);
        }else{
            response.setFirst(first);
            response.setLast(last);
        }
        return response;        
    }


    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata= metadata;
    }

}
