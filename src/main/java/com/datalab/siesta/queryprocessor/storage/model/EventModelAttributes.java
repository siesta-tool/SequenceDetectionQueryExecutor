package com.datalab.siesta.queryprocessor.storage.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class EventModelAttributes implements Serializable{
    private String traceId;
    private String eventName;
    private String timestamp;
    private int position;
    private Map<String, String> attributes;
}
