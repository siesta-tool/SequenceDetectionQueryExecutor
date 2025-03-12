package com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.declare.model.EventSupport;
import com.datalab.siesta.queryprocessor.declare.model.PairConstraint;
import com.datalab.siesta.queryprocessor.declare.model.declareState.NegativeState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.OrderState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateU;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelationsState;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryOrderRelationWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;


import scala.Tuple3;

@Component
@RequestScope
public class QueryPlanOrderRelationsState extends QueryPlanState {

    private DBConnector dbConnector;
    private SparkSession sparkSession;

    public QueryPlanOrderRelationsState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext,
                                        DBConnector dbConnector, SparkSession sparkSession) {
        super(declareDBConnector, javaSparkContext);
        this.dbConnector = dbConnector;
        this.sparkSession = sparkSession;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryOrderRelationWrapper qow = (QueryOrderRelationWrapper) qw;

        //get all possible activities from database to create activity matrix
        List<String> activities = dbConnector.getEventNames(metadata.getLogname());
        Dataset<String> activityDF = sparkSession.createDataset(activities, Encoders.STRING());
        Dataset<Row> activityMatrix = activityDF.crossJoin(activityDF).toDF("activityA", "activityB");

        //Extract All constraints
        List<PairConstraint> constraints = this.extractAll(qow.getSupport(), activityMatrix);

        //filter the constraints to create the response
        QueryResponseOrderedRelationsState response = new QueryResponseOrderedRelationsState();
        this.setSpecificConstraints(qow, constraints, response);

        this.extractStatistics(qow);
        response.setUpToDate(qow.isStateUpToDate());
        if (!qow.isStateUpToDate()) {
            response.setEventsPercentage(((double) qow.getIndexedEvents() / metadata.getEvents()) * 100);
            response.setTracesPercentage(((double) qow.getIndexedTraces() / metadata.getTraces()) * 100);
            response.setMessage("State is not fully updated. Consider re-running the preprocess to get 100% accurate constraints");
        } else {
            response.setEventsPercentage(100);
            response.setTracesPercentage(100);
        }

        return response;
    }

    public QueryResponseOrderedRelationsState extractConstraintFunction(List<PairConstraint> constraints, String mode) {
        QueryResponseOrderedRelationsState response = new QueryResponseOrderedRelationsState();
        response.setMode(mode);
        for (PairConstraint uc : constraints) {
            if (mode.equals("simple")) {
                if (uc.getRule().equals("response")) {
                    response.getResponse().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("precedence")) {
                    response.getPrecedence().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("succession")) {
                    response.getSuccession().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("not-succession")) {
                    response.getNotSuccession().add(uc.getEventPairSupport());
                }
            } else {
                if (uc.getRule().contains("response") && uc.getRule().contains(mode)) {
                    response.getResponse().add(uc.getEventPairSupport());
                } else if (uc.getRule().contains("precedence") && uc.getRule().contains(mode)) {
                    response.getPrecedence().add(uc.getEventPairSupport());
                } else if (uc.getRule().contains("succession") && uc.getRule().contains(mode)) {
                    response.getSuccession().add(uc.getEventPairSupport());
                }
                if (mode.equals("alternate") && uc.getRule().equals("not-succession")) {
                    response.getNotSuccession().add(uc.getEventPairSupport());
                } else if (mode.equals("chain") && uc.getRule().equals("not-chain-succession")) {
                    response.getNotSuccession().add(uc.getEventPairSupport());
                }
            }

        }
        return response;

    }

    public List<PairConstraint> extractAll(double support, Dataset<Row> activityMatrix) {
        //load order constraints from the database
        Dataset<OrderState> orderStateDF = this.declareDBConnector.queryOrderState(this.metadata.getLogname());
        // activityMatrix has format <activityA, activityB>
        Dataset<Row> filteredActivities = activityMatrix
                .filter(functions.col("activityA").notEqual(functions.col("activityB")));

        Dataset<Row> onlyChains = orderStateDF
                .filter(functions.col("rule").contains("chain"))
                .select("eventA", "eventB")
                .distinct();

        Dataset<Row> remainingPairs = filteredActivities
                .join(onlyChains, functions.col("activityA").equalTo(functions.col("eventA"))
                        .and(functions.col("activityB").equalTo(functions.col("eventB"))), "left_anti");

        List<PairConstraint> constraints0 = remainingPairs.collectAsList()
                .stream().map(row -> new PairConstraint(
                        new EventPairSupport(row.getString(0), row.getString(1), 1.0),
                        "not-chain-succession")).toList();

        List<PairConstraint> constraints = new ArrayList<>(constraints0);
        //get unique traces per event type
        Dataset<EventSupport> eventOccurrencesDF = this.declareDBConnector.querySingleTable(this.metadata.getLogname());
        Map<String, Double> eventOccurrences = eventOccurrencesDF.collectAsList().stream()
                .collect(Collectors.toMap(
                        EventSupport::getEvent,
                        EventSupport::getSupport
                ));
        List<OrderState> orderStateList = orderStateDF.collectAsList();
        List<PairConstraint> allConstraints = orderStateList.parallelStream().
                flatMap(x -> {
                    List<PairConstraint> l = new ArrayList<>();
                    double sup = x.getOccurrences() / eventOccurrences.get(x.getEventB());
                    if (x.getRule().contains("response")) {
                        sup = x.getOccurrences() / eventOccurrences.get(x.getEventA());
                    }
                    l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), sup), x.getRule()));
                    if (x.getRule().contains("chain")) {
                        l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), sup), "chain-succession"));
                        l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), 1 - sup), "not-chain-succession"));
                    } else if (x.getRule().contains("alternate")) {
                        l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), sup), "alternate-succession"));
                    } else {
                        l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), sup), "succession"));
                        l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), 1 - sup), "not-succession"));
                    }
                    return l.parallelStream();
                })
                .toList();

        Map<Tuple3<String, String, String>, PairConstraint> mergedConstraints = new ConcurrentHashMap<>();

        allConstraints.parallelStream().forEach(pc -> {
            Tuple3<String, String, String> key = new Tuple3<>(pc.getRule(), pc.getEventPairSupport().getEventA(), pc.getEventPairSupport().getEventB());

            mergedConstraints.merge(key, pc, (existing, newValue) -> {
                EventPairSupport eps = existing.getEventPairSupport();
                eps.setSupport(eps.getSupport() * newValue.getEventPairSupport().getSupport()); // Multiply support values
                return new PairConstraint(eps, existing.getRule());
            });
        });

        List<PairConstraint> constraints2 = mergedConstraints.values().parallelStream()
                .filter(pc -> pc.getEventPairSupport().getSupport() >= support)
                .toList();
        constraints.addAll(constraints2);

        //handle negatives
        Dataset<NegativeState> negativeStateDF = this.declareDBConnector.queryNegativeState(this.metadata.getLogname());
        List<PairConstraint> constraints3 = negativeStateDF.collectAsList()
                .parallelStream().map(row -> {
                    EventPairSupport eps = new EventPairSupport(row.get_1(), row.get_2(), 1.0);
                    return new PairConstraint(eps, "not-succession");
                }).toList();
        constraints.addAll(constraints3);
        //return all the constraints to be filtered
        return constraints;
    }

    private void setSpecificConstraints(QueryOrderRelationWrapper qow, List<PairConstraint> constraints,
                                        QueryResponseOrderedRelationsState response) {
        response.setMode(qow.getMode());
        // Wrapper has 3 modes simple, alternate and chain. 
        // Based on these values this methods keeps the correct constraints.
        for (PairConstraint uc : constraints) {
            if (qow.getMode().equals("simple")) {
                if (uc.getRule().equals("response") && qow.getConstraint().contains(uc.getRule())) {
                    response.getResponse().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("precedence") && qow.getConstraint().contains(uc.getRule())) {
                    response.getPrecedence().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("succession") && qow.getConstraint().contains(uc.getRule())) {
                    response.getSuccession().add(uc.getEventPairSupport());
                } else if (uc.getRule().contains("not-succession")) {
                    response.getNotSuccession().add(uc.getEventPairSupport());
                }
            } else if (qow.getMode().equals("alternate")) {
                if (uc.getRule().equals("alternate-succession") && qow.getConstraint().contains("alternate-succession")) {
                    response.getSuccession().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("not-succession")) {
                    response.getNotSuccession().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("alternate-response") && qow.getConstraint().contains("response")) {
                    response.getResponse().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("alternate-precedence") && qow.getConstraint().contains("precedence")) {
                    response.getPrecedence().add(uc.getEventPairSupport());
                }
            } else if (qow.getMode().equals("chain")) {
                if (uc.getRule().equals("chain-succession") && qow.getConstraint().contains("succession")) {
                    response.getSuccession().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("not-chain-succession")) {
                    response.getNotSuccession().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("chain-response") && qow.getConstraint().contains("response")) {
                    response.getResponse().add(uc.getEventPairSupport());
                } else if (uc.getRule().equals("chain-precedence") && qow.getConstraint().contains("precedence")) {
                    response.getPrecedence().add(uc.getEventPairSupport());
                }
            }

        }
    }


}
