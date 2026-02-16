package com.datalab.siesta.queryprocessor.declare.queryPlans.existence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.EventN;
import com.datalab.siesta.queryprocessor.declare.model.EventPairSupport;
import com.datalab.siesta.queryprocessor.declare.model.ExistenceConstraint;
import com.datalab.siesta.queryprocessor.declare.model.declareState.ExistenceState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateI;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateU;
import com.datalab.siesta.queryprocessor.declare.queryPlans.QueryPlanState;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistenceState;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryExistenceWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import com.datalab.siesta.queryprocessor.declare.model.UnorderedHelper;
import com.datalab.siesta.queryprocessor.declare.model.PairConstraint;

import org.apache.spark.sql.*;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import org.springframework.beans.factory.annotation.Autowired;



@Component
@RequestScope
public class QueryPlanExistancesState extends QueryPlanState {

    private final SparkSession sparkSession;
    private DBConnector dbConnector;

    @Autowired
    public QueryPlanExistancesState(DeclareDBConnector declareDBConnector, JavaSparkContext javaSparkContext, DBConnector dbConnector, SparkSession sparkSession) {
        super(declareDBConnector, javaSparkContext);
        this.dbConnector = dbConnector;
        this.sparkSession = sparkSession;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryExistenceWrapper qew = (QueryExistenceWrapper) qw;

        //get all possible activities from database to create activity matrix
        List<String> activities = dbConnector.getEventNames(metadata.getLogname());
        Dataset<String> activityDF = sparkSession.createDataset(activities, Encoders.STRING());
        Dataset<Row> activityMatrix = activityDF.crossJoin(activityDF).toDF("activityA", "activityB");

        QueryResponseExistenceState response = this.extractConstraintsFunction(qew.getSupport(), metadata.getTraces(),
                activityMatrix, qew);

        this.extractStatistics(qew);
        response.setUpToDate(qew.isStateUpToDate());
        if (!qew.isStateUpToDate()) {
            response.setEventsPercentage(((double) qew.getIndexedEvents() / metadata.getEvents()) * 100);
            response.setTracesPercentage(((double) qew.getIndexedTraces() / metadata.getTraces()) * 100);
            response.setMessage("State is not fully updated. Consider re-running the preprocess to get 100% accurate constraints");
        } else {
            response.setEventsPercentage(100);
            response.setTracesPercentage(100);
        }

        return response;
    }

    public QueryResponseExistenceState extractConstraintsFunction(double support, long traces, Dataset<Row> activityMatrix,
                                                                  QueryExistenceWrapper qew) {
        QueryResponseExistenceState response = new QueryResponseExistenceState();

        String[] existenceConstraints = {"existence", "absence", "exactly"};
        if (Arrays.stream(existenceConstraints).anyMatch(qew.getModes()::contains)) {
            this.calculateExistence(qew.getModes(), response, traces, support);
        }

        String[] unorderedConstraints = {"co-existence", "not-co-existence", "choice",
                "exclusive-choice", "responded-existence"};
        if (Arrays.stream(unorderedConstraints).anyMatch(qew.getModes()::contains)) {
            this.calculateUnordered(qew.getModes(), response, traces, support, activityMatrix);
        }
        return response;
    }

    private void calculateExistence(List<String> modes, QueryResponseExistence qre, long traces, double support) {
        List<ExistenceState> existences = declareDBConnector.queryExistenceState(metadata.getLogname())
                .collectAsList();

        List<ExistenceConstraint> c = existences.parallelStream()
                .collect(Collectors.groupingByConcurrent(ExistenceState::getEvent_type,
                        ConcurrentHashMap::new, Collectors.toList()))
                .entrySet().parallelStream()
                .flatMap(entry -> {
                    List<ExistenceConstraint> constraints = new ArrayList<>();
                    String eventType = entry.getKey();
                    List<ExistenceState> sortedActivities = entry.getValue();
                    // sort ascending by occurrences
                    sortedActivities.sort(Comparator.comparingInt(ExistenceState::getOccurrences));

                    for (ExistenceState activity : sortedActivities) {
                        if ((double) activity.getContained() / traces >= support) {
                            constraints.add(new ExistenceConstraint("exactly", eventType, activity.getOccurrences(),
                                    (double) activity.getContained() / traces));
                        }
                    }

                    //Existence Constraints
                    Collections.reverse(sortedActivities); //reverse the list so higher is lower
                    long cumulativeOccurrences = sortedActivities.get(0).getContained();
                    int pos = 0;
                    //this loop will start from the higher number of occurrences and go down to one. In each iteration
                    //will calculate the number of traces that contain at least x instances of the activity
                    for (int occurrences = sortedActivities.get(0).getOccurrences(); occurrences >= 1; occurrences--) {
                        //the occurrences are equal to the next one smaller occurrence
                        if (pos + 1 < sortedActivities.size() && occurrences == sortedActivities.get(pos + 1).getOccurrences()) {
                            pos += 1;
                            cumulativeOccurrences += sortedActivities.get(pos).getContained();
                        }
                        if ((double) cumulativeOccurrences / traces >= support) {
                            constraints.add(new ExistenceConstraint("existence", eventType, occurrences,
                                    (double) cumulativeOccurrences / traces));
                        }
                    }

                    // Absence constraint
                    Collections.reverse(sortedActivities); // smaller to greater
                    pos = -1;
                    int cumulativeAbsence = (int) (traces - sortedActivities.stream().mapToLong(ExistenceState::getContained).sum());
                    for (int absence = 1; absence <= 3; absence++) {
                        if ((double) cumulativeAbsence / traces >= support) {
                            constraints.add(new ExistenceConstraint("absence", eventType, absence,
                                    (double) cumulativeAbsence / traces));
                        }
                        if (pos + 1 < sortedActivities.size() && absence == sortedActivities.get(pos + 1).getOccurrences()) {
                            pos += 1;
                            cumulativeAbsence += sortedActivities.get(pos).getContained();
                        }

                    }
                    return constraints.parallelStream();
                })
                .toList();

        if (modes.contains("existence")) {
            qre.setExistence(getExistenceToResponse("existence", c));
        }
        if (modes.contains("absence")) {
            qre.setAbsence(getExistenceToResponse("absence", c));
        }
        if (modes.contains("exactly")) {
            qre.setExactly(getExistenceToResponse("exactly", c));
        }
    }

    private List<EventN> getExistenceToResponse(String rule, List<ExistenceConstraint> constraints) {
        return constraints.stream().filter(x -> {
            return x.getRule().equals(rule);
        }).map(x -> {
            return new EventN(x.getEvent_type(), x.getN(), x.getOccurrences());
        }).collect(Collectors.toList());
    }

    private void calculateUnordered(List<String> modes, QueryResponseExistence qre, long traces,
                                    double support, Dataset<Row> activityMatrix) {


        Dataset<UnorderStateI> iTable = declareDBConnector.queryUnorderStateI(metadata.getLogname());
        Dataset<UnorderStateU> uTable = declareDBConnector.queryUnorderStateU(metadata.getLogname()).as("uTable");
        Dataset<Row> iTablePrepared = iTable
                .withColumn("key_i", functions.concat(functions.col("`_1`"), functions.col("`_2`")))
                .as("iTable");

        //activity matrix is <activityA, activityB>
        // Extract the unordered constraints by merging the activity matrix - iTable - uTable
        List<UnorderedHelper> unorderedHelpers = activityMatrix.as("activityMatrix")
                .join(uTable.as("uTable"),
                        functions.col("activityMatrix.activityA").equalTo(functions.col("uTable.`_1`")), "left")
                .selectExpr("activityMatrix.activityA as eventA", "activityMatrix.activityB as eventB", "uTable.`_2` as ua")
                .join(uTable.as("uTable2"),
                        functions.col("eventB").equalTo(functions.col("uTable2.`_1`")), "left")
                .selectExpr("eventA", "eventB", "ua", "uTable2.`_2` as ub")
                .withColumn("key", functions.when(functions.col("eventA").geq("eventB"),
                                functions.concat(functions.col("eventA"), functions.col("eventB")))
                        .otherwise(functions.concat(functions.col("eventB"), functions.col("eventA"))))
                .join(iTablePrepared.as("iTable"), functions.col("key").equalTo(functions.col("key_i")), "left")
                .selectExpr("eventA", "eventB", "key", "ua", "ub", "iTable.`_3` as pairs")
                .na().fill(0)
                .distinct()
                .as(Encoders.bean(UnorderedHelper.class))
                .collectAsList();

        List<PairConstraint> unorderedConstraints = unorderedHelpers.parallelStream()
                .flatMap(x -> {
                    List<PairConstraint> l = new ArrayList<>();
                    long r = traces - x.getUa() + x.getPairs();
                    l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), r), "responded-existence"));
                    if (x.getEventA().compareTo(x.getEventB()) < 0) {
                        r = x.getUa() + x.getUb() - x.getPairs();
                        l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), r), "choice"));
                        r = traces - x.getUa() - x.getUb() + 2 * x.getPairs();
                        l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), r), "co-existence"));
                        //exclusive_choice = total - co-existen
                        l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), traces - r), "exclusive-choice"));
                        //not-existence : traces where a exist and not b, traces where b exists and not a, traces where neither occur
                        r = traces - x.getPairs();
                        l.add(new PairConstraint(new EventPairSupport(x.getEventA(), x.getEventB(), r), "not-co-existence"));
                    }
                    return l.parallelStream();
                })
                .filter(x -> (x.getEventPairSupport().getSupport() /
                        (double) traces) >= support)
                .map(x -> {
                    EventPairSupport eps = new EventPairSupport();
                    eps.setEventA(x.getEventPairSupport().getEventA());
                    eps.setEventB(x.getEventPairSupport().getEventB());
                    eps.setSupport(x.getEventPairSupport().getSupport() / (double) traces);
                    return new PairConstraint(eps, x.getRule());
                }).toList();

        if (modes.contains("responded-existence")) {
            qre.setRespondedExistence(getUnorderToResponse("responded-existence", unorderedConstraints));
        }
        if (modes.contains("choice")) {
            qre.setChoice(getUnorderToResponse("choice", unorderedConstraints));
        }
        if (modes.contains("co-existence")) {
            qre.setCoExistence(getUnorderToResponse("co-existence", unorderedConstraints));
        }
        if (modes.contains("exclusive-choice")) {
            qre.setExclusiveChoice(getUnorderToResponse("exclusive-choice", unorderedConstraints));
        }
        if (modes.contains("not-co-existence")) {
            qre.setNotCoExistence(getUnorderToResponse("not-co-existence", unorderedConstraints));
        }

    }

    private List<EventPairSupport> getUnorderToResponse(String rule, List<PairConstraint> constraints) {
        return constraints.stream().filter(x -> {
                    return x.getRule().equals(rule);
                })
                .map(PairConstraint::getEventPairSupport)
                .collect(Collectors.toList());
    }

}
