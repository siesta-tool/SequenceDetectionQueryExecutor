package com.datalab.siesta.queryprocessor.model.Events;

import com.datalab.siesta.queryprocessor.SaseConnection.SaseEvent;
import com.datalab.siesta.queryprocessor.model.Serializations.EventBothSerializer;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.MappingJacksonViews;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.*;

/**
 * A SIESTA event that contains both time and position information
 */
@JsonSerialize(using = EventBothSerializer.class)
public class EventBoth extends EventTs implements Comparable, Serializable {

    @JsonView(MappingJacksonViews.EventAllInfo.class)
    private int position;

    @JsonView(MappingJacksonViews.EventAllInfo.class)
    private Map<String, String> attributes; // New attributes field

    public EventBoth() {
        this.position = -1;
        this.attributes = new HashMap<>(); // Initialize empty attributes map
    }

    public EventBoth(String name, Timestamp ts, int pos) {
        super(name, ts);
        this.position = pos;
        this.attributes = new HashMap<>();
    }

    public EventBoth(String name, String traceID, Timestamp timestamp, int position) {
        super(name, traceID, timestamp);
        this.position = position;
        this.attributes = new HashMap<>();
    }

    public EventBoth(String name, String traceID, Timestamp timestamp, int position, Map<String, String> attributes) {
        super(name, traceID, timestamp);
        this.position = position;
        this.attributes = attributes != null ? attributes : new HashMap<>();
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    @JsonIgnore
    public SaseEvent transformSaseEvent(int position) {
        SaseEvent se = super.transformSaseEvent(position);
        se.setTimestamp((int) this.timestamp.getTime() / 1000); // Transform to seconds
        return se;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof EventBoth) {
            return this.timestamp.compareTo(((EventBoth) o).getTimestamp());
        } else if (o instanceof EventTs) {
            return this.timestamp.compareTo(((EventTs) o).getTimestamp());
        } else if (o instanceof EventPos) {
            return Integer.compare(this.position, ((EventPos) o).getPosition());
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EventBoth eventBoth = (EventBoth) o;
        return position == eventBoth.position &&
                Objects.equals(attributes, eventBoth.attributes); // Include attributes in comparison
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), position, attributes);
    }

    @Override
    public long calculateDiff(Event e) { // Return the diff in seconds
        return (((EventTs) e).getTimestamp().getTime() - this.timestamp.getTime()) / 1000;
    }

    @Override
    @JsonIgnore
    public long getPrimaryMetric() {
        return this.timestamp.getTime() / 1000;
    }

    @Override
    public void setPrimaryMetric(long newPrimaryMetric) {
        this.timestamp = new Timestamp(newPrimaryMetric);
    }
}