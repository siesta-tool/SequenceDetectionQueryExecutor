package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.DeclareUtilities;
import com.datalab.siesta.queryprocessor.declare.model.Abstract2OrderConstraint;
import com.datalab.siesta.queryprocessor.declare.model.EventPairTraceOccurrences;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

@Component
@RequestScope
public class QueryPlanOrderedRelationsChain extends QueryPlanOrderedRelations{

    public QueryPlanOrderedRelationsChain(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                                          DeclareUtilities declareUtilities) {
        super(declareDBConnector, javaSparkContext, declareUtilities);
    }

    @Override
    public void initQueryResponse() {
        this.queryResponseOrderedRelations = new QueryResponseOrderedRelations("chain");
    }

    @Override
    public Dataset<Abstract2OrderConstraint> evaluateConstraint(Dataset<EventPairTraceOccurrences> joined) {
        Dataset<Row> response = joined
                .withColumn("s_r", functions.expr(
                        "size(filter(occurrencesA, a -> exists(occurrencesB, y -> y = a+1)))"
                ))
                .selectExpr("eventA", "eventB", "'r' as type", "s_r as count"); // Precedence constraint

        Dataset<Row> precedence = joined
                .withColumn("s_p", functions.expr(
                        "size(filter(occurrencesB, b -> exists(occurrencesA, y -> y = b-1)))"
                ))
                .selectExpr("eventA", "eventB", "'p' as type", "s_p as count");
        Dataset<Abstract2OrderConstraint> unioned = response.union(precedence)
                .groupBy("eventA", "eventB", "type")
                .agg(functions.sum("count").alias("occurrences"))
                .withColumn("occurrences", functions.col("occurrences").cast("int"))
                .selectExpr("eventA", "eventB", "type as mode", "occurrences")
                .as(Encoders.bean(Abstract2OrderConstraint.class));

        return unioned;
    }
}
