package com.datalab.siesta.queryprocessor.declare.model;

import com.datalab.siesta.queryprocessor.model.Events.EventPair;

import java.io.Serializable;
import java.util.List;

public class UniqueTracesPerEventPair implements Serializable {

    private String eventA;
    private String eventB;
    private List<Long> uniqueTraces;

    public UniqueTracesPerEventPair(String eventA, String eventB, List<Long> uniqueTraces) {
        this.eventA = eventA;
        this.eventB = eventB;
        this.uniqueTraces = uniqueTraces;
    }

    public String getEventA() {
        return eventA;
    }

    public void setEventA(String eventA) {
        this.eventA = eventA;
    }

    public String getEventB() {
        return eventB;
    }

    public void setEventB(String eventB) {
        this.eventB = eventB;
    }

    public List<Long> getUniqueTraces() {
        return uniqueTraces;
    }

    public void setUniqueTraces(List<Long> uniqueTraces) {
        this.uniqueTraces = uniqueTraces;
    }
}
