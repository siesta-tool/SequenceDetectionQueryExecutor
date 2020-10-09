package com.sequence.detection.rest.query;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.Futures;
import com.sequence.detection.rest.model.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The class responsible for accurately evaluating a query. All the initial candidates are then tested and any false positives are discarded
 *
 * @author Andreas Kosmatopoulos
 */
public class SequenceQueryEvaluator extends SequenceQueryHandler {

    /**
     * Constructor
     *
     * @param cluster                 The cluster instance
     * @param session                 The session instance
     * @param ks                      The keyspace metadata instance
     * @param cassandra_keyspace_name The keyspace name
     */
    public SequenceQueryEvaluator(Cluster cluster, Session session, KeyspaceMetadata ks, String cassandra_keyspace_name) {
        super(cluster, session, ks, cassandra_keyspace_name);
    }

    /**
     * Overrides the function below and executes queries only in a table, provided from the funnel
     *
     * @param start_date Funnel start date
     * @param end_date   Funnel end date
     * @param query      Query to be executed
     * @param allDetails The details accompanying the query
     * @param tableName  The name of the log file
     * @return a set of potential candidate user/device IDs
     */
    public Set<String> evaluateQueryLogFile(Date start_date, Date end_date, Sequence query, Map<Integer, List<AugmentedDetail>> allDetails, String tableName) {
        List<String> allCandidates = new ArrayList<String>();
        List<QueryPair> query_tuples = query.getQueryTuples();
        if (query_tuples.isEmpty())
            return new HashSet<>(allCandidates); // Which is empty
        allEventsPerSession = new ConcurrentHashMap<>();
        if (ks.getTable(tableName) == null)
            return new HashSet<>(allCandidates); // Which is empty

        Set<String> candidates = executeQuery(tableName, query_tuples, query, start_date, end_date, allDetails);

        allCandidates.addAll(candidates);
        return new HashSet<>(allCandidates);
    }


    private Set<String> executeQuery(String tableName, List<QueryPair> query_tuples, Sequence query, Date start_date, Date end_date, Map<Integer, List<AugmentedDetail>> allDetails) {
        final ExecutorService epThread = Executors.newSingleThreadExecutor();
        final ExecutorService detThread = Executors.newSingleThreadExecutor();

        final CountDownLatch doneSignal; // The countdown will reach zero once all threads have finished their task
        doneSignal = new CountDownLatch(Math.max(query_tuples.size() + allDetails.size() - 1, 0));

        ResultSet rs = session.execute("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event1_name = ? AND event2_name = ? ", query_tuples.get(0).getFirst().getName(), query_tuples.get(0).getSecond().getName());

        Row row = rs.one();

        List<String> candSessionsFull, candSessions;
        if (row != null) {
            candSessionsFull = row.getList("sequences", String.class);
            candSessions = handleSequenceRow(query, query_tuples.get(0).getFirst().getName(), query_tuples.get(0).getSecond().getName(), start_date, end_date, candSessionsFull, true);
        } else
            candSessions = new ArrayList<>();
        Set<String> candidates = Collections.synchronizedSet(new HashSet<String>(candSessions));
        for (QueryPair ep : query_tuples) // Query (async) for all (but the first) event triples of the query
        {
            if (query_tuples.get(0) == ep)
                continue;

            ResultSetFuture resultSetFuture = session.executeAsync("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event1_name = ? AND event2_name = ?", ep.getFirst().getName(), ep.getSecond().getName());
            Futures.addCallback(resultSetFuture, new CandSessionsCallback(candidates, query, ep.getFirst().getName(), ep.getSecond().getName(), start_date, end_date, doneSignal, "sequences"), epThread);
        }
        for (Integer stepCount : allDetails.keySet()) {
            List<AugmentedDetail> stepDetails = allDetails.get(stepCount);
            for (AugmentedDetail det : stepDetails) // Query (async) for all the details of the funnel
            {
                ResultSetFuture resultSetFuture = session.executeAsync("SELECT " + "sequences" + " FROM " + cassandra_keyspace_name + "." + tableName + " WHERE event1_name = ? AND event2_name = ? AND third_field = ?", det.getFirst(), det.getSecond(), det.getThird());
                Futures.addCallback(resultSetFuture, new CandSessionsCallback(candidates, query, det.getFirst(), det.getSecond(), start_date, end_date, doneSignal, "sequences"), detThread);
            }
        }

        try {
            doneSignal.await(); // Wait until all async queries have finished
        } catch (InterruptedException ex) {
            Logger.getLogger(SequenceQueryEvaluator.class.getName()).log(Level.SEVERE, null, ex);
        }

        epThread.shutdown();
        detThread.shutdown();

        return candidates;
    }


    /**
     * For each candidate we check for two requirements: 1) The events of that candidate occur in the correct order,
     * 2) They occur within the timeframe of the "maxDuration" variable
     *
     * @param query       The query sequence
     * @param candidates  A list of call candidates
     * @param maxDuration The max duration timeframe
     * @return A set of all true positives along with their completion duration
     */
    public Map<String, Long> findTruePositives(Sequence query, Set<String> candidates, long maxDuration) {
        Map<String, Long> truePositives = new HashMap<>();
        long maxDate = 0;
        for (String candidate : candidates) {
            List<TimestampedEvent> allEvents = allEventsPerSession.get(candidate);
            Collections.sort(allEvents);
            long[] timestamps = query.fitsTimestamps(allEvents, maxDuration);
            if (timestamps[0] == -1 && timestamps[1] == -1)
                continue;
            truePositives.put(candidate, timestamps[1] - timestamps[0]);
            if (timestamps[1] > maxDate)
                maxDate = timestamps[1];
        }
        truePositives.put("maxDate", maxDate);
        return truePositives;
    }

    /**
     * For each candidate we check for two requirements: 1) The events of that candidate occur in the correct order,
     * 2) They occur within the timeframe of the "maxDuration" variable
     *
     * @param query       The query sequence
     * @param candidates  A list of call candidates
     * @param maxDuration The max duration timeframe
     * @return A set of all true positives along with their Lifetime
     */
    public Map<String, Lifetime> findTruePositivesAndLifetime(Sequence query, Set<String> candidates, long maxDuration) {
        Map<String, Lifetime> truePositives = new HashMap<>();
        long maxDate = 0;
        for (String candidate : candidates) {
            List<TimestampedEvent> allEvents = allEventsPerSession.get(candidate);
            Collections.sort(allEvents);
            long[] timestamps = query.fitsTimestamps(allEvents, maxDuration);
            if (timestamps[0] == -1 && timestamps[1] == -1)
                continue;
            truePositives.put(candidate, new Lifetime(new Date(timestamps[0]), new Date(timestamps[1]), timestamps[1] - timestamps[0]));
            if (timestamps[1] > maxDate)
                maxDate = timestamps[1];
        }
        return truePositives;
    }
}