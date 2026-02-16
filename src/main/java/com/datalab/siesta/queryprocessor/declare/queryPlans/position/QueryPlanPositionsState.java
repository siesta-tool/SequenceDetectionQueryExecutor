package com.datalab.siesta.queryprocessor.declare.queryPlans.position;

import java.util.List;

import com.datalab.siesta.queryprocessor.declare.model.EventSupport;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.declareState.PositionState;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePositionState;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryPositionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;



@Component
@RequestScope
public class QueryPlanPositionsState extends QueryPlanState {

    public QueryPlanPositionsState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext){
        super(declareDBConnector,javaSparkContext);
    }


    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryPositionWrapper qpw = (QueryPositionWrapper) qw;

        QueryResponsePositionState response = this.extractConstraintsFunction(qpw.getSupport(), metadata.getTraces(), qpw);
        
        this.extractStatistics(qpw);
        response.setUpToDate(qpw.isStateUpToDate());
        if(!qpw.isStateUpToDate()){
            response.setEventsPercentage(((double) qpw.getIndexedEvents() /metadata.getEvents())*100);
            response.setTracesPercentage(((double) qpw.getIndexedTraces() /metadata.getTraces())*100);
            response.setMessage("State is not fully updated. " +
                    "Consider re-running the preprocess to get 100% accurate constraints");
        }else{
            response.setEventsPercentage(100);
            response.setTracesPercentage(100);
        }

        return response;
    }

    public QueryResponsePositionState extractConstraintsFunction(double support, long traces, QueryPositionWrapper qpw){
        Dataset<PositionState> data = this.declareDBConnector.queryPositionState(metadata.getLogname());
        Dataset<Row> filtered = data.withColumn("support", functions.col("occurrences").divide(traces))
                .filter(functions.col("support").geq(support));

        List<EventSupport> firsts = filtered
                .filter(functions.col("rule").equalTo("first"))
                .selectExpr("event_type as event","support")
                .as(Encoders.bean(EventSupport.class))
                .collectAsList();

        List<EventSupport> lasts = filtered
                .filter(functions.col("rule").equalTo("last"))
                .selectExpr("event_type as event","support")
                .as(Encoders.bean(EventSupport.class))
                .collectAsList();

        QueryResponsePositionState response = new QueryResponsePositionState();

        if(qpw.getMode().equals("first")){
            response.setFirst(firsts);
        }else if(qpw.getMode().equals("last")){
            response.setLast(lasts);
        }else{
            response.setFirst(firsts);
            response.setLast(lasts);
        }
        return response;
    }
    

}
