package com.datalab.siesta.queryprocessor.declare.queryPlans;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.declareState.ExistenceState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.PositionState;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryWrapperDeclare;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

@Component
@RequestScope
public class QueryPlanState  implements QueryPlan{

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
    public QueryPlanState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext) {
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        throw new UnsupportedOperationException("Unimplemented method 'execute'");
    }

    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Extracts statistics about events and traces indexed in the declare states. It utilizes the
     * positions and declare states to extract the accurate number of indexed traces and events
     * and evaluates if the declare states are up to date or not. The QueryWrapper is then 
     * used by the QueryPlanner to specify how accurate the extracted constraints are.
     */
    public void extractStatistics(QueryWrapperDeclare qwd){
        Dataset<PositionState> ps = declareDBConnector.queryPositionState(qwd.getLog_name());
        int traces_stated = ps.filter(functions.col("rule").equalTo("first"))
                .select("occurrences")
                .agg(functions.sum("occurrences").cast("int"))
                .as(Encoders.INT())
                .collectAsList()
                .get(0);
        qwd.setIndexedTraces(traces_stated);

        Dataset<ExistenceState> es = declareDBConnector.queryExistenceState(qwd.getLog_name());
        int events_stated = es.withColumn("total",functions.col("occurrences")
                        .multiply(functions.col("contained")))
                .agg(functions.sum("total").cast("int"))
                .as(Encoders.INT())
                .collectAsList()
                .get(0);

        qwd.setIndexedEvents(events_stated);

        if(qwd.getIndexedEvents()==metadata.getEvents()){
            qwd.setStateUpToDate(true);
        }
    }

}
