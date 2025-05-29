package com.datalab.siesta.queryprocessor.model.DBModel;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

/**
 * A record of the IndexTable. Stores information about:
 * - the trace if
 * - the names of the events (eventA, eventB)
 * - the timestamps of the events (timestampA, timestampB)
 * - the position of the events in the trace (positionA, positionB)
 * Note that depending on the metadata, IndexTable will contain only one of the timestamps/positions. Therefore
 * it is expected the other fields to be empty (null/-1 respectively). If both information is required to answer a
 * query, they can be retrieved from SequenceTable (which contains both).
 */
@Getter
@Setter
@AllArgsConstructor
public class IndexPair implements Serializable {
    private String trace_id;
    private String eventA;
    private String eventB;
    private String timestampA;
    private String timestampB;
    private int positionA;
    private int positionB;


    public IndexPair() {
        this.eventA = "";
        this.eventB = "";
        this.positionA = -1;
        this.positionB = -1;
        this.timestampA = null;
        this.timestampB = null;
    }

    public IndexPair(String traceId, String eventA, String eventB, String timestampA, String timestampB) {
        this.trace_id = traceId;
        this.positionA = -1;
        this.positionB = -1;
        this.eventA = eventA;
        this.eventB = eventB;
        this.timestampA = timestampA;
        this.timestampB = timestampB;
    }

    public IndexPair(String traceId, String eventA, String eventB, int positionA, int positionB) {
        this.trace_id = traceId;
        this.timestampA = null;
        this.timestampB = null;
        this.eventA = eventA;
        this.eventB = eventB;
        this.positionA = positionA;
        this.positionB = positionB;
    }

    @JsonIgnore
    public boolean validate(Set<EventPair> pairs){
        for(EventPair p:pairs){
            if(p.getEventA().getName().equals(this.eventA)&&p.getEventB().getName().equals(this.eventB)) return true;
        }
        return false;
    }

    public long getDuration() { return (Timestamp.valueOf(timestampB).getTime() - Timestamp.valueOf(timestampA).getTime()) / 1000; }
}
