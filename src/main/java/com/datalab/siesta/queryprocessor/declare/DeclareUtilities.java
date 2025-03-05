package com.datalab.siesta.queryprocessor.declare;

import com.datalab.siesta.queryprocessor.declare.model.EventPairToNumberOfTrace;
import com.datalab.siesta.queryprocessor.model.DBModel.EventTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.jvnet.hk2.annotations.Service;
import org.springframework.stereotype.Component;

import java.util.*;

@Service
@Component
public class DeclareUtilities {

    public DeclareUtilities() {
    }

    /**
     * Extract the pairs not found in the database
     * @param eventTypes a list of all the event types
     * @param joined a rdd containing all the event pairs that occurred
     * @return a set of all the event pairs that did not appear in the log database
     */
    public Set<EventTypes> extractNotFoundPairs(Set<String> eventTypes, Dataset<EventPairToNumberOfTrace> joined) {

        //calculate all the event pairs (n^2) and store them in a set
        //event pairs of type (eventA,eventA) are excluded
        Set<EventTypes> allEventPairs = new HashSet<>();
        for (String eventA : eventTypes) {
            for (String eventB : eventTypes) {
                if (!eventA.equals(eventB)) {
                    allEventPairs.add(new EventTypes(eventA, eventB));
                }
            }
        }
        //removes from the above set all the event pairs that have at least one occurrence in the
        List<EventTypes> foundEventPairs = joined
                .select("eventA","eventB")
                .as(Encoders.bean(EventTypes.class))
                .collectAsList();

        foundEventPairs.forEach(allEventPairs::remove);
        return allEventPairs;
    }
}
