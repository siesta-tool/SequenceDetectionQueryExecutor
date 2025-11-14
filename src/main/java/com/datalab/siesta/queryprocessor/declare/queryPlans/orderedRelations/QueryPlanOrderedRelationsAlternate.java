package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.DeclareUtilities;
import com.datalab.siesta.queryprocessor.declare.model.Abstract2OrderConstraint;
import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.declare.model.EventPairTraceOccurrences;
import com.datalab.siesta.queryprocessor.declare.model.EventTypeOccurrences;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import java.util.ArrayList;
import java.util.List;

@Component
@RequestScope
public class QueryPlanOrderedRelationsAlternate extends QueryPlanOrderedRelations {
    public QueryPlanOrderedRelationsAlternate(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                                              DeclareUtilities declareUtilities) {
        super(declareDBConnector, javaSparkContext, declareUtilities);
    }

    @Override
    public void initQueryResponse() {
        this.queryResponseOrderedRelations = new QueryResponseOrderedRelations("alternate");
    }

    @Override
    public Dataset<Abstract2OrderConstraint> evaluateConstraint(Dataset<EventPairTraceOccurrences> joined) {
        Dataset<Row> response = joined
                // sort the list of occurrences A
                .withColumn("sortedA", functions.expr("array_sort(occurrencesA)"))
                // split list into 2 consecutive pairs
                .withColumn("consecutivePairs",
                        functions.expr("arrays_zip(sortedA, slice(sortedA, 2, size(sortedA)-1))"))
                // count elements in occurrencesB that fall between consecutive pairs
                .withColumn("s_r1", functions.expr(
                        "size(filter(consecutivePairs, pair -> exists(occurrencesB, y -> y > pair.sortedA AND y < pair.`1`)))"
                ))
                // finally check if there is any event greater than the last one
                .withColumn("lastA", functions.expr("element_at(sortedA, -1)"))
                .withColumn("s_r2", functions.expr(
                        "CASE WHEN exists(occurrencesB, y-> y>lastA) THEN 1 ELSE 0 END"
                ))
                // determine the final occurrences
                .withColumn("summary", functions.expr("s_r1 + s_r2"))
                // select the appropriate fields for Abstract2OrderConstraint
                .selectExpr("eventA", "eventB", "'r' as type", "summary");

        Dataset<Row> precedence = joined
                // sort the list of occurrences A
                .withColumn("sortedB", functions.expr("array_sort(occurrencesB)"))
                // split list into 2 consecutive pairs
                .withColumn("consecutivePairs",
                        functions.expr("arrays_zip(sortedB, slice(sortedB, 2, size(sortedB)-1))"))
                // count elements in occurrencesB that fall between consecutive pairs
                .withColumn("s_r1", functions.expr(
                        "size(filter(consecutivePairs, pair -> exists(occurrencesA, y -> y > pair.sortedB AND y < pair.`1`)))"
                ))
                // finally check if there is any event smaller than the first one
                .withColumn("firstA", functions.expr("element_at(sortedB, 1)"))
                .withColumn("s_r2", functions.expr(
                        "CASE WHEN exists(occurrencesA, y-> y<firstA) THEN 1 ELSE 0 END"
                ))
                // determine the final occurrences
                .withColumn("summary", functions.expr("s_r1 + s_r2"))
                // select the appropriate fields for Abstract2OrderConstraint
                .selectExpr("eventA", "eventB", "'p' as type", "summary");

        Dataset<Abstract2OrderConstraint> unioned = response.union(precedence)
                .groupBy("eventA", "eventB", "type")
                .agg(functions.sum("summary").alias("occurrences"))
                .withColumn("occurrences", functions.col("occurrences").cast("int"))
                .selectExpr("eventA", "eventB", "type as mode", "occurrences")
                .as(Encoders.bean(Abstract2OrderConstraint.class));

        return unioned;
    }

    /**
     * This method is similar to the one in the super class, with the only difference that it does not
     * calculate notSuccession relations
     * @param c           the occurrences of different templates detected
     * @param eventTypeOccurrences a spark broadcast map fo the form (event type) -> total occurrences in the log database
     * @param support     the user-defined support
     */
    @Override
    public void filterBasedOnSupport(Dataset<Abstract2OrderConstraint> c,
                                     Dataset<EventTypeOccurrences>  eventTypeOccurrences, double support) {
        Dataset<Row> intermediate = super.getIntermediate(eventTypeOccurrences,c);

        intermediate.persist(StorageLevel.MEMORY_AND_DISK());

        // filters based on the user-defined support and collect the results
        Dataset<Row> detected = intermediate.filter(functions.col("support").geq(support));
        List<EventPairSupport> responses = detected
                .filter(functions.col("mode").equalTo("r"))
                .selectExpr("eventA", "eventB", "support")
                .as(Encoders.bean(EventPairSupport.class))
                .collectAsList();

        List<EventPairSupport> precedence = detected
                .filter(functions.col("mode").equalTo("p"))
                .selectExpr("eventA", "eventB", "support")
                .as(Encoders.bean(EventPairSupport.class))
                .collectAsList();

        if (!precedence.isEmpty() && !responses.isEmpty()) { //we are looking for succession and no succession
            setResults(responses, "response");
            setResults(precedence, "precedence");
            //handle succession (both "r" and "p" of these events should have a support greater than the user-defined
            List<EventPairSupport> succession = new ArrayList<>();
            for (EventPairSupport r1 : responses) {
                for (EventPairSupport p1 : precedence) {
                    if (r1.getEventA().equals(p1.getEventA()) && r1.getEventB().equals(p1.getEventB())) {
                        double s = r1.getSupport() * p1.getSupport();
                        EventPairSupport ep = new EventPairSupport(r1.getEventA(), r1.getEventB(), s);
                        succession.add(ep);
                        break;
                    }
                }
            }
            setResults(succession, "succession");
        }else if (precedence.isEmpty()) {
            setResults(responses, "response");
        } else {
            setResults(precedence, "precedence");
        }
        intermediate.unpersist();
    }
}
