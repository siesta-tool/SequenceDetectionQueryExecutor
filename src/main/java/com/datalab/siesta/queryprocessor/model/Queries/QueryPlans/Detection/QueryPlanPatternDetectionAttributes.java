package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Detection;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseConnector;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.Occurrence;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponsePatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springdoc.core.converters.ResponseSupportConverter;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

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
            filterByAttributeEquality((QueryResponsePatternDetection) response);
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

        Set<String> allKeys = new HashSet<>();

        for (Map<String, String> innerMap : attributes.values()) {
            allKeys.addAll(innerMap.keySet());
        }

        if (equalAttributes != null) {
            allKeys.addAll(equalAttributes.keySet());
        }

        Map<String, List<EventBoth>> result = dbConnector.querySeqTable(
                this.metadata.getLogname(),
                new ArrayList<>(traceIds),
                eventTypes,
                null,
                null,
                allKeys
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

    private void filterByAttributeEquality(QueryResponsePatternDetection response) {
        // Apply additional filtering logic based on attributes
        response.setOccurrences(
                response.getOccurrences().stream()
                        .filter(this::meetsEqualityCriteria)
                        .collect(Collectors.toList())
        );
    }

    private boolean meetsEqualityCriteria(Occurrences occurrences) {
        if (equalAttributes == null || equalAttributes.isEmpty()) {
            return true;
        }


        for (Occurrence occurrence : occurrences.getOccurrences()) {
            List<EventBoth> events = occurrence.getOccurrence();
            for (String attributeKey : equalAttributes.keySet()) {

                List<Integer> equalPositions = equalAttributes.get(attributeKey);

                if (equalPositions == null || equalPositions.isEmpty()) continue;

                String attributeValue = events.get(equalPositions.get(0)).getAttributes().get(attributeKey);
                for (int position : equalPositions) {
                    EventBoth event = events.get(position);
                    if (event.getAttributes() == null || event.getAttributes().get(attributeKey) == null || !event.getAttributes().get(attributeKey).equals(attributeValue)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}
