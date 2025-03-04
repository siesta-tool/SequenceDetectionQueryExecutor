package com.datalab.siesta.queryprocessor.model.DBModel;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;

/**
 * The class that represents an et-pair without the conditions.
 * @see com.datalab.siesta.queryprocessor.model.Events.EventPair (for implementation with conditions)
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class EventTypes implements Serializable{
    private String eventA;

    private String eventB;

    @Override
    public String toString() {
        return eventA + eventB;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventTypes that = (EventTypes) o;
        return eventA.equals(that.eventA) && eventB.equals(that.eventB);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventA, eventB);
    }

}
