package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A SIESTA event that contains only position information
 */
public class EventPos extends Event implements Serializable, Comparable, Cloneable {

    @JsonView(MappingJacksonViews.EventAllInfo.class)
    protected int position;

    @JsonView(MappingJacksonViews.EventAllInfo.class)
    protected Map<String, String> attributes;

    public EventPos() {
        this.position=-1;
        this.traceID="";
        this.attributes = new HashMap<>();
    }

    public EventPos(String name, int pos) {
        super(name);
        this.position=pos;
    }

    public EventPos(String name, String traceID, int position) {
        super(name, traceID);
        this.position = position;
    }

    public EventPos(String name, String traceID, int position, Map<String,String> attributes) {
        super(name, traceID);
        this.position = position;
        this.attributes = attributes;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public Map<String,String> getAttributes() {return attributes;}

    public void setAttributes(Map<String,String> attributes) {this.attributes = attributes;}

    @Override
    @JsonIgnore
    public EventBoth getEventBoth(){
        return new EventBoth(this.name,null,null,this.position, this.attributes);
    }

    @Override
    @JsonIgnore
    public SaseEvent transformSaseEvent(int position) {
        SaseEvent se = super.transformSaseEvent(position);
        se.setPosition(this.position);
        return se;
    }

    @Override
    public int compareTo(Object o) {
        EventPos ep = (EventPos) o;
        return Integer.compare(this.position,ep.position);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EventPos eventPos = (EventPos) o;
        return position == eventPos.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), position);
    }

    @Override
    public long calculateDiff(Event e) {
        return ((EventPos) e).getPosition()-this.position;
    }

    @Override
    @JsonIgnore
    public long getPrimaryMetric() {
        return this.position;
    }

    @Override
    public void setPrimaryMetric(long newPrimaryMetric) {
        this.position= (int) newPrimaryMetric;
    }
    @Override
    public EventPos clone() {
        return new EventPos(name,traceID,position);
    }

}