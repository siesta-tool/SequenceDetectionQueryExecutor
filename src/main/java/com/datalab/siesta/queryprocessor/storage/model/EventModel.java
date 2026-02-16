package com.datalab.siesta.queryprocessor.storage.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class EventModel implements Serializable{
    private String traceId;
    private String eventName;
    private String timestamp;
    private int position;
}
