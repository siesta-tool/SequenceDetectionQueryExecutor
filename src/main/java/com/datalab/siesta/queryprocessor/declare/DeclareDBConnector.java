package com.datalab.siesta.queryprocessor.declare;

import com.datalab.siesta.queryprocessor.declare.model.*;
import com.datalab.siesta.queryprocessor.declare.model.declareState.ExistenceState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.NegativeState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.OrderState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.PositionState;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateI;
import com.datalab.siesta.queryprocessor.declare.model.declareState.UnorderStateU;
import com.datalab.siesta.queryprocessor.model.DBModel.IndexPair;
import com.datalab.siesta.queryprocessor.storage.model.EventTypeTracePositions;
import com.datalab.siesta.queryprocessor.storage.model.Trace;
import com.datalab.siesta.queryprocessor.storage.DatabaseRepository;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DeclareDBConnector {

    private DatabaseRepository db;

    @Autowired
    public DeclareDBConnector(DatabaseRepository databaseRepository){
        this.db=databaseRepository;
    }

    public Dataset<Trace> querySequenceTableDeclare(String logName){
        return db.querySequenceTableDeclare(logName);
    }

    public Dataset<UniqueTracesPerEventType> querySingleTableDeclare(String logname){
        return this.db.querySingleTableDeclare(logname);
    }

    public Dataset<EventSupport> querySingleTable(String logname){
        return this.db.querySingleTable(logname);
    }


    public Dataset<UniqueTracesPerEventPair> queryIndexTableDeclare(String logname){
        return this.db.queryIndexTableDeclare(logname);
    }

    public JavaRDD<IndexPair> queryIndexTableAllDeclare(String logname){
        return this.db.queryIndexTableAllDeclare(logname);
    }

    public Dataset<EventTypeTracePositions> querySingleTableAllDeclare(String logname){
        return this.db.querySingleTableAllDeclare(logname);
    }


    public Dataset<EventPairToTrace> queryIndexOriginalDeclare(String logname){
        return this.db.queryIndexOriginalDeclare(logname);
    }

    public Dataset<EventTypeOccurrences> extractTotalOccurrencesPerEventType(String logname){
        Dataset<EventTypeOccurrences> eventTypeOccurrencesDataset=  this.querySingleTableDeclare(logname)
                .withColumn("numberOfTraces", functions.expr(
                        "aggregate(occurrences, 0, (acc, a) -> acc + a.occs)"
                ))
                .selectExpr("eventName", "numberOfTraces")
                .as(Encoders.bean(EventTypeOccurrences.class));
        return eventTypeOccurrencesDataset;
    }

    public JavaRDD<PositionState> queryPositionState(String logname){
        return this.db.queryPositionState(logname);
    }

    public JavaRDD<ExistenceState> queryExistenceState(String logname){
        return this.db.queryExistenceState(logname);
    }

    public JavaRDD<UnorderStateI> queryUnorderStateI(String logname){
        return this.db.queryUnorderStateI(logname);
    }
    public JavaRDD<UnorderStateU> queryUnorderStateU(String logname){
        return this.db.queryUnorderStateU(logname);
    }
    public JavaRDD<OrderState> queryOrderState(String logname){
        return this.db.queryOrderState(logname);
    }
    public JavaRDD<NegativeState> queryNegativeState(String logname){
        return this.db.queryNegativeState(logname);
    }




}
