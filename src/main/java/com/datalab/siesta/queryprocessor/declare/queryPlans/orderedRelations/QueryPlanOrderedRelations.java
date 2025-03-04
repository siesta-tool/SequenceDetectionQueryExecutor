package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.DeclareUtilities;
import com.datalab.siesta.queryprocessor.declare.model.*;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryOrderRelationWrapper;
import com.datalab.siesta.queryprocessor.model.DBModel.EventTypes;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

import com.datalab.siesta.queryprocessor.storage.model.EventTypeTracePositions;
import lombok.Getter;
import lombok.Setter;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;


import java.util.*;


@Component
@RequestScope
public class QueryPlanOrderedRelations implements QueryPlan {

    @Setter
    protected Metadata metadata;
    protected DeclareDBConnector declareDBConnector;
    protected JavaSparkContext javaSparkContext;
    @Getter
    protected QueryResponseOrderedRelations queryResponseOrderedRelations;
    protected OrderedRelationsUtilityFunctions utils;
    protected DeclareUtilities declareUtilities;

    @Autowired
    public QueryPlanOrderedRelations(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                                     OrderedRelationsUtilityFunctions utils, DeclareUtilities declareUtilities) {
        this.declareDBConnector = declareDBConnector;
        this.javaSparkContext = javaSparkContext;
        this.utils = utils;
        this.declareUtilities = declareUtilities;
    }

    public void initQueryResponse() {
        this.queryResponseOrderedRelations = new QueryResponseOrderedRelations("simple");
    }


    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryOrderRelationWrapper qpw = (QueryOrderRelationWrapper) qw;
        //query IndexTable
        Dataset<EventPairToTrace> indexRDD = declareDBConnector.queryIndexOriginalDeclare(this.metadata.getLogname())
                .filter(functions.col("eventA").notEqual("eventB"));
        //query SingleTable
        Dataset<EventTypeTracePositions> singleRDD = declareDBConnector
                .querySingleTableAllDeclare(this.metadata.getLogname());

        //join tables using joinTables and flat map to get the single events
        Dataset<EventPairTraceOccurrences> joined = joinTables(indexRDD, singleRDD);
        joined.persist(StorageLevel.MEMORY_AND_DISK());

        //count the occurrences using the evaluate constraints
        Dataset<Abstract2OrderConstraint> c = evaluateConstraint(joined, qpw.getConstraint());
        //filter based on the values of the SingleTable and the provided support and write to the response
        Dataset<EventTypeOccurrences> eventTypeOccurrencesDataset = declareDBConnector
                .extractTotalOccurrencesPerEventType(metadata.getLogname());
        //add the not-succession constraints detected from the event pairs that did not occur in the database log
        extendNotSuccession(eventTypeOccurrencesDataset, this.metadata.getLogname(), c);
        //filter based on the user defined support
        filterBasedOnSupport(c, eventTypeOccurrencesDataset, qpw.getSupport());
        joined.unpersist();
        return this.queryResponseOrderedRelations;
    }

    /**
     * for the traces that contain an occurrence of the event pair (a,b), joins their occurrences of these
     * event types (extracted from the Single Table).
     *
     * @param indexRDD  a rdd of {@link EventPairToTrace}
     * @param singleRDD a rdd of records that have the format (event
     * @ a rdd of {@link EventPairTraceOccurrences}
     */
    public Dataset<EventPairTraceOccurrences> joinTables(Dataset<EventPairToTrace> indexRDD,
                                                         Dataset<EventTypeTracePositions> singleRDD) {
        //for the traces that contain an occurrence of the event pair (a,b), joins their occurrences of these
        //event types (extracted from the Single Table)

        Dataset<Row> singleTransformed = singleRDD.
                selectExpr("eventName", "traceId as trace_id", "positions");// Alias for first join

        Dataset<EventPairTraceOccurrences> joined = indexRDD.as("primary")
                .join(singleTransformed.as("singleA"), functions.col("primary.eventA")
                        .equalTo(functions.col("singleA.eventName"))
                        .and(functions.col("primary.trace_id")
                                .equalTo(functions.col("singleA.trace_id"))), "inner")
                .selectExpr("eventA", "eventB", "primary.trace_id", "singleA.positions as positionsA")
                .join(singleTransformed.as("singleB"), functions.col("primary.eventB")
                        .equalTo(functions.col("singleB.eventName"))
                        .and(functions.col("primary.trace_id")
                                .equalTo(functions.col("singleB.trace_id"))), "inner")
                .selectExpr("eventA", "eventB", "primary.trace_id as traceId",
                        "positionsA as occurrencesA", "singleB.positions as occurrencesB")
                .as(Encoders.bean(EventPairTraceOccurrences.class));

        return joined;
    }

    /**
     * Calculates the number of occurrences for each pattern per trace, depending on the different
     * constraint type.
     *
     * @param joined     a rdd of {@link EventPairTraceOccurrences}
     * @param constraint a string that describes the constraint under evaluation 'response', 'precedence'
     *                   or 'succession' (which is the default execution)
     * @return a rdd of {@link  Abstract2OrderConstraint}
     */
    public Dataset<Abstract2OrderConstraint> evaluateConstraint
    (Dataset<EventPairTraceOccurrences> joined, String constraint) {
        Dataset<Row> response = joined
                .withColumn("s_r", functions.expr(
                        "size(filter(occurrencesA, a -> exists(occurrencesB, y -> y > a)))"
                ))
                .selectExpr("eventA", "eventB", "'r' as type", "s_r as count"); // Precedence constraint

        Dataset<Row> precedence = joined
                .withColumn("s_p", functions.expr(
                        "size(filter(occurrencesB, b -> exists(occurrencesA, y -> y < b)))"
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


    /**
     * filters based on support and constraint required and write them to the response
     *
     * @param c           the occurrences of different templates detected
     * @param eventTypeOccurrences a spark broadcast map fo the form (event type) -> total occurrences in the log database
     * @param support     the user-defined support
     */
    public void filterBasedOnSupport(Dataset<Abstract2OrderConstraint> c,
                                     Dataset<EventTypeOccurrences> eventTypeOccurrences, double support) {

        //calculates the support based on either the total occurrences of the first event (response)
        //or the occurrences of the second event (precedence)
        Dataset<Row> firstEvent = eventTypeOccurrences.as("et")
                .withColumnRenamed("eventName", "eventA")
                .withColumnRenamed("numberOfTraces", "totalA");

        Dataset<Row> secondEvent = eventTypeOccurrences.as("et2")
                .withColumnRenamed("eventName", "eventB")
                .withColumnRenamed("numberOfTraces", "totalB");

        Dataset<Row> joinedDf = c.as("primary")
                .join(firstEvent.as("et"),
                        functions.col("primary.eventA").equalTo(functions.col("et.eventA")), "left")
                .join(secondEvent.as("et2"),
                        functions.col("primary.eventB").equalTo(functions.col("et2.eventB")), "left");

        Dataset<Row> intermediate = joinedDf
                .withColumn("total", functions.expr(
                        "CASE WHEN mode = 'r' THEN totalA ELSE totalB END"
                ))
                .withColumn("support", functions.col("occurrences").cast("double").divide(functions.col("total")))
                .select("mode", "primary.eventA", "primary.eventB", "support");

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
            //handle no succession
            //event pairs where both "r" and "p" have support less than the user-defined
            //for the pairs that do not appear here the function extendNotSuccession() is executed - check below

            Dataset<Row> notSuccession = intermediate
                    .filter(functions.col("support").leq(1-support));

            List<EventPairSupport> notSuccessionR = notSuccession
                    .filter(functions.col("mode").equalTo("r"))
                    .selectExpr("eventA", "eventB", "support")
                    .as(Encoders.bean(EventPairSupport.class))
                    .collectAsList();

            List<EventPairSupport> notSuccessionP = notSuccession
                    .filter(functions.col("mode").equalTo("p"))
                    .selectExpr("eventA", "eventB", "support")
                    .as(Encoders.bean(EventPairSupport.class))
                    .collectAsList();

            //create the event pair support based on the above event pairs
            List<EventPairSupport> notSuccessionList = new ArrayList<>();
            for (EventPairSupport r1 : notSuccessionR) {
                for (EventPairSupport p1 : notSuccessionP) {
                    if (r1.getEventA().equals(p1.getEventA()) && r1.getEventB().equals(p1.getEventB())) {
                        double s = (1 - r1.getSupport()) * (1 - p1.getSupport());
                        EventPairSupport ep = new EventPairSupport(r1.getEventA(), r1.getEventB(), s);
                        notSuccessionList.add(ep);
                        break;
                    }
                }
            }
            //set the different lists to the appropriate lists
            setResults(notSuccessionList, "not-succession");
        } else if (precedence.isEmpty()) {
            setResults(responses, "response");
        } else {
            setResults(precedence, "precedence");
        }
        intermediate.unpersist();
    }


    /**
     * Sets the different list of constraints to the appropriate fields in the response
     *
     * @param results    a list containing detected constraints
     * @param constraint the mode (response/precedence/succession/not-succession)
     */
    protected void setResults(List<EventPairSupport> results, String constraint) {
        switch (constraint) {
            case "response":
                queryResponseOrderedRelations.setResponse(results);
                break;
            case "precedence":
                queryResponseOrderedRelations.setPrecedence(results);
                break;
            case "succession":
                queryResponseOrderedRelations.setSuccession(results);
                break;
            case "not-succession":
                queryResponseOrderedRelations.setNotSuccession(results);
                break;
        }
    }

    /**
     * Detects not succession patterns from the not found event pairs
     *
     * @param uEventType a map of the form (event type) -> total occurrences in the database log
     * @param logname    the name of the log database (used to load information from the database)
     * @param cSimple    the extracted constraints, a rdd of {@link Abstract2OrderConstraint}
     */
    public void extendNotSuccession(Dataset<EventTypeOccurrences> uEventType, String logname,
                                    Dataset<Abstract2OrderConstraint> cSimple) {
        // since the first argument may not be available it will be loaded and calculated from the database
        if (uEventType == null) {
            uEventType = declareDBConnector.extractTotalOccurrencesPerEventType(logname);
        }
        //transform rdd to a compatible version to be used by the extractNotFoundPairs
        Dataset<EventPairToNumberOfTrace> mappedRdd = cSimple
                .selectExpr("eventA", "eventB", "1 as numberOfTraces")
                .as(Encoders.bean(EventPairToNumberOfTrace.class));

        Set<String> keys = new HashSet<>(uEventType.select("eventName").as(Encoders.STRING()).collectAsList());
        Set<EventTypes> notFound = declareUtilities.extractNotFoundPairs(keys, mappedRdd);
        //transform event pairs to event pairs with support (which is 100% by definition)
        List<EventPairSupport> result = notFound.stream().map(x -> new EventPairSupport(x.getEventA(),
                x.getEventB(), 1)).toList();
        this.queryResponseOrderedRelations.setNotSuccession(result);
    }


}
