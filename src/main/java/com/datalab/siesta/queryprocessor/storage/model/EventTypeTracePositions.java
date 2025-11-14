package com.datalab.siesta.queryprocessor.storage.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class EventTypeTracePositions implements Serializable {
    private String EventName;
    private String traceId;
    private List<Integer> positions;
}
