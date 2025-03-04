package com.datalab.siesta.queryprocessor.declare.model;

import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class UniqueTracesPerEventPair implements Serializable {

    private String eventA;
    private String eventB;
    private List<String> uniqueTraces;

    public Tuple2<String,String> getKey(){
        return new Tuple2<>(this.eventA,this.eventB);
    }

    public Tuple2<String,String> getKeyReverse(){
        return new Tuple2<>(this.eventB,this.eventA);
    }
}
