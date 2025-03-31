package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Detection;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.Constraints.Constraint;
import com.datalab.siesta.queryprocessor.model.Constraints.GapConstraint;
import com.datalab.siesta.queryprocessor.model.Constraints.TimeConstraint;
import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexMiddleResult;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Occurrence;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Patterns.SIESTAPattern;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseBadRequestForDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponsePatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.TimeStats;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdoc.core.converters.ResponseSupportConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The query plan of the pattern detection query
 */
@Component
//@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@RequestScope
public class QueryPlanPatternDetectionAttributes extends QueryPlanPatternDetection {

    private final ResponseSupportConverter responseSupportConverter;

    public QueryPlanPatternDetectionAttributes(DBConnector dbConnector, SaseConnector saseConnector, Utils utils, ResponseSupportConverter responseSupportConverter) {
        super(dbConnector, saseConnector, utils);
        this.responseSupportConverter = responseSupportConverter;
    }

    @Override
    public QueryResponse execute(QueryWrapper qw) {
        QueryResponse response = super.execute(qw);
        if (response instanceof QueryResponsePatternDetection) {
            this.getEventAttributes((QueryResponsePatternDetection) response);
            filterByAttributes((QueryResponsePatternDetection) response);
        }
        return response;
    }

    private void getEventAttributes(QueryResponsePatternDetection response) {
        List<Occurrences> responseOccurrences = response.getOccurrences();

        Set<String> traceIds = responseOccurrences.stream()
                .map(Occurrences::getTraceID)
                .collect(Collectors.toSet());

        Set<String> eventTypes = responseOccurrences.stream()
                .flatMap(occurrences -> occurrences.getOccurrences().stream())
                .flatMap(occurrence -> occurrence.getOccurrence().stream())
                .map(EventBoth::getName)
                .collect(Collectors.toSet());

        Map<String, List<EventBoth>> result = dbConnector.querySeqTable(
                this.metadata.getLogname(),
                new ArrayList<>(traceIds),
                eventTypes,
                null,
                null
        );

        // Update the occurrences with the results
        for (Occurrences occurrences : responseOccurrences) {
            List<EventBoth> dbEvents = result.get(occurrences.getTraceID());
            if (dbEvents == null) continue;

            Map<Integer, EventBoth> dbEventMap = dbEvents.stream()
                    .collect(Collectors.toMap(EventBoth::getPosition, e -> e));

            for (Occurrence occurrence : occurrences.getOccurrences()) {
                for (EventBoth event : occurrence.getOccurrence()) {
                    EventBoth matchedEvent = dbEventMap.get(event.getPosition());
                    if (matchedEvent != null) {
                        event.setAttributes(matchedEvent.getAttributes());
                    }
                }
            }
        }
    }

    private void filterByAttributes(QueryResponsePatternDetection response) {
        // Apply additional filtering logic based on attributes
        response.setOccurrences(
                response.getOccurrences().stream()
                        .filter(this::meetsAttributeCriteria)
                        .collect(Collectors.toList())
        );
    }

    private boolean meetsAttributeCriteria(Occurrences occurrences) {
        if (attributes == null || attributes.isEmpty()) {
            return true;
        }

        for (Occurrence occurrence : occurrences.getOccurrences()) {
            List<EventBoth> events = occurrence.getOccurrence(); // Get list of events in occurrence

            if (events.size() < attributes.size()) {
                continue;
            }

            boolean allMatch = true;

            for (int index = 0; index < events.size() && index < attributes.size(); index++) {
                EventBoth event = events.get(index);
                Map<String, String> eventAttributes = event.getAttributes();
                Map<String, String> criteriaAttributes = attributes.get(index);

                // If there is a criteria for this index, check attributes match
                if (criteriaAttributes != null && !criteriaAttributes.isEmpty()) {
                    if (eventAttributes == null || !eventAttributes.entrySet().containsAll(criteriaAttributes.entrySet())) {
                        allMatch = false;
                        break; // No need to check further
                    }
                }
            }

            if (allMatch) {
                return true;
            }
        }
        return false;
    }
}
