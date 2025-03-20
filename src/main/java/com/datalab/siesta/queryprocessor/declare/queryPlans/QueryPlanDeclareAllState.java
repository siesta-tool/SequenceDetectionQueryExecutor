package com.datalab.siesta.queryprocessor.declare.queryPlans;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.PairConstraint;
import com.datalab.siesta.queryprocessor.declare.queryPlans.position.QueryPlanPositionsState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseDeclareAllState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistenceState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelationsState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePositionState;
import com.datalab.siesta.queryprocessor.declare.queryPlans.existence.*;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderRelationsState;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryExistenceWrapper;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryPositionWrapper;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryWrapperDeclare;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;

@Component
@RequestScope
public class QueryPlanDeclareAllState extends QueryPlanState{

    private QueryPlanPositionsState queryPlanPositionsState;
    private QueryPlanExistancesState queryPlanExistencesState;
    private QueryPlanOrderRelationsState queryPlanOrderedRelationsState;
    private DBConnector dbConnector;
    private SparkSession sparkSession;


    @Autowired
    public QueryPlanDeclareAllState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                                    DBConnector dbConnector,
                                    QueryPlanPositionsState queryPlanPositionsState, QueryPlanExistancesState queryPlanExistencesState,
                                    QueryPlanOrderRelationsState queryPlanOrderedRelationsState, SparkSession sparkSession){
            super(declareDBConnector,javaSparkContext);
            this.queryPlanPositionsState = queryPlanPositionsState;
            this.queryPlanExistencesState = queryPlanExistencesState;
            this.queryPlanOrderedRelationsState = queryPlanOrderedRelationsState;
            this.dbConnector = dbConnector;
            this.sparkSession = sparkSession;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryWrapperDeclare qdw = (QueryWrapperDeclare) qw;
        double support = qdw.getSupport();
        long traces = this.metadata.getTraces();
        // run extract positions
        QueryPositionWrapper qpw = new QueryPositionWrapper(qdw.getSupport());
        qpw.setMode("both");
        this.queryPlanPositionsState.setMetadata(this.metadata);
        QueryResponsePositionState prpState = this.queryPlanPositionsState.extractConstraintsFunction(support,
                traces,qpw);

        // run extract existences
        QueryExistenceWrapper qew = new QueryExistenceWrapper(qdw.getSupport());
        String[] modes = {"existence","absence","exactly","co-existence","not-co-existence", "choice",
                "exclusive-choice", "responded-existence"};
        qew.setModes(new ArrayList<>(Arrays.asList(modes)));
        List<String> activities = dbConnector.getEventNames(metadata.getLogname());
        Dataset<String> activityDF = sparkSession.createDataset(activities, Encoders.STRING());
        Dataset<Row> activityMatrix = activityDF.crossJoin(activityDF).toDF("activityA", "activityB");

        this.queryPlanExistencesState.setMetadata(this.metadata);
        QueryResponseExistenceState preState = this.queryPlanExistencesState.extractConstraintsFunction(support,traces,activityMatrix,qew);
        this.extractStatistics(qdw);

        // run extract ordered relations
        this.queryPlanOrderedRelationsState.setMetadata(this.metadata);
        List<PairConstraint> pairConstraints = this.queryPlanOrderedRelationsState.extractAll(support, activityMatrix);
        QueryResponseOrderedRelationsState simple = this.queryPlanOrderedRelationsState.extractConstraintFunction(pairConstraints, "simple");
        QueryResponseOrderedRelationsState alternate = this.queryPlanOrderedRelationsState.extractConstraintFunction(pairConstraints, "alternate");
        QueryResponseOrderedRelationsState chain = this.queryPlanOrderedRelationsState.extractConstraintFunction(pairConstraints, "chain");

        QueryResponseDeclareAllState response = new QueryResponseDeclareAllState(prpState.getQueryResponsePosition(), preState.getQueryResponseExistence()
        , simple.getQueryResponseOrderedRelations(), alternate.getQueryResponseOrderedRelations(), chain.getQueryResponseOrderedRelations());

        this.extractStatistics(qdw);
        response.setUpToDate(qdw.isStateUpToDate());
        if(!qdw.isStateUpToDate()){
            response.setEventsPercentage(((double) qdw.getIndexedEvents() /metadata.getEvents())*100);
            response.setTracesPercentage(((double) qdw.getIndexedTraces() /metadata.getTraces())*100);
            response.setMessage("State is not fully updated. Consider re-running the preprocess to get 100% accurate constraints");
        }else{
            response.setEventsPercentage(100);
            response.setTracesPercentage(100);
        }

        return response;
    }

}
