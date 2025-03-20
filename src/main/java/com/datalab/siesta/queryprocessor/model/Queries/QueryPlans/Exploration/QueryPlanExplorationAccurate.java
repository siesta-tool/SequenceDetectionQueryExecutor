package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Exploration;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Events.EventPos;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Occurrence;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Patterns.SimplePattern;
import com.datalab.siesta.queryprocessor.model.Proposition;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Detection.QueryPlanPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseExploration;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryExploreWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The query plan for the accurate detection of continuation for the query pattern
 */
@Component
@RequestScope
public class QueryPlanExplorationAccurate extends QueryPlanPatternDetection implements QueryPlan {

    protected HashMap<String, List<String>> calculatedPairs;

    @Autowired
    public QueryPlanExplorationAccurate(DBConnector dbConnector, SaseConnector saseConnector, Utils utils) {
        super(dbConnector, saseConnector, utils);
        this.calculatedPairs = new HashMap<>();
    }

    /**
     * Using the CountTable, the next possible events are retrieved. Then for each possible next event the pattern detection
     * query runs in order to determine the exact number of traces that contain the complete pattern.
     * Finally, the propositions were added in the response and are sorted from the most frequent next event to the least
     * frequent
     *
     * @param qw the QueryPatternDetectionWrapper
     * @return the possible next events sorted based on frequency, in the form of propositions
     */
    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryExploreWrapper queryExploreWrapper = (QueryExploreWrapper) qw;
        List<EventPos> events = queryExploreWrapper.getPattern().getEvents();
        String lastEvent = events.get(events.size() - 1).getName();
        List<Count> freqs = dbConnector.getCountForExploration(queryExploreWrapper.getLog_name(), lastEvent);
        List<Proposition> props = new ArrayList<>();
        for (Count freq : freqs) {
            try {
                SimplePattern sp = (SimplePattern) queryExploreWrapper.getPattern().clone();
                Proposition p = this.patternDetection(sp, freq.getEventB(), qw.getLog_name());
                if (p != null) props.add(p);
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }

        }
        props.sort(Collections.reverseOrder());
        return new QueryResponseExploration(props);
    }

    /**
     * This will append the second event from the count to the simple pattern and the detect the exact occurrences of
     * this pattern in the dataset
     *
     * @param pattern the original pattern we want to explore future events
     * @param next    a count pair that contains the last event of the pattern and one possible extension
     * @return a proposition
     */
    protected Proposition patternDetection(SimplePattern pattern, String next, String logname) {
        List<EventPos> events = pattern.getEvents();
        events.add(new EventPos(next, events.size()));
        pattern.setEvents(events); //create the pattern
        ExtractedPairsForPatternDetection pairs = pattern.extractPairsForPatternDetection(false);
        //Todo: check if we need all the pairs or simply the consecutive ones
        List<Count> sortedPairs = this.getStats(pairs.getAllPairs(), metadata.getLogname());
        List<Tuple2<EventPair, Count>> combined = this.combineWithPairs(pairs.getAllPairs(), sortedPairs);
        imr = dbConnector.patterDetectionTraceIds(metadata.getLogname(), combined, metadata, pairs, null, null);
        //retrieve time information from the SequenceTable
        super.retrieveTimeInformation(pattern, logname, null, null);
        List<Occurrences> occurrences = saseConnector.evaluate(pattern, imr.getEvents(), false);
        occurrences.forEach(x -> x.clearOccurrences(true));
        List<Occurrence> ocs = occurrences.stream().parallel().flatMap(x -> x.getOccurrences().stream()).collect(Collectors.toList());
        if (!ocs.isEmpty()) {
            double avg_duration = ocs.stream().mapToDouble(Occurrence::getDuration).sum() / ocs.size();
            return new Proposition(next, ocs.size(), avg_duration);
        } else {
            return null;
        }
    }

    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }
}
