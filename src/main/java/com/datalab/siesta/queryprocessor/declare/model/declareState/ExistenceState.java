package com.datalab.siesta.queryprocessor.declare.model.declareState;

import java.io.Serializable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class ExistenceState implements Serializable{

    private String event_type;
    private int occurrences;
    private long contained;

}
