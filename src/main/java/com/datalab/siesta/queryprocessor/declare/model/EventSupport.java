package com.datalab.siesta.queryprocessor.declare.model;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class EventSupport {

    @JsonProperty("ev")
    protected String event;
    @JsonProperty("support")
    @JsonSerialize(using = SupportSerializer.class)
    protected Double support;
}

