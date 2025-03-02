package com.datalab.siesta.queryprocessor.model.DBModel;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * A record in the CountTable. Contains the et-pair (eventA, eventB), the minimum and maximum duration (calculated
 * based on the time distance between every event-pair of these events, the number of event-pairs in the database and the
 * sum of all the durations (used to calculate mean duration)
 */
@Getter
@Setter
public class Count implements Serializable {

    private String eventA;

    private String eventB;

    private long sumDuration;

    private int count;

    private long minDuration;

    private long maxDuration;

    private double sumSquares;

    public Count() {
    }

    public Count(String eventA, String eventB, long sum_duration, int count, long min_duration, long max_duration, double sum_squares) {
        this.eventA = eventA;
        this.eventB = eventB;
        this.sumDuration = sum_duration;
        this.count = count;
        this.minDuration = min_duration;
        this.maxDuration = max_duration;
        this.sumSquares = sum_squares;
    }

    public Count(String eventA, String[] record) {
        this.eventA = eventA;
        this.eventB = record[0];
        this.sumDuration = Long.parseLong(record[1]);
        this.count = Integer.parseInt(record[2]);
        this.minDuration = Long.parseLong(record[3]);
        this.maxDuration = Long.parseLong(record[4]);
        this.sumSquares = Double.parseDouble(record[5]);
    }


    public String getPair() {
        return this.eventA + this.eventB;
    }

    @Override
    public String toString() {
        return "Count{" +
                "eventA='" + eventA + '\'' +
                ", eventB='" + eventB + '\'' +
                ", sum_duration=" + sumDuration +
                ", count=" + count +
                ", min_duration=" + minDuration +
                ", max_duration=" + maxDuration +
                ", sum_squares=" + sumSquares +
                '}';
    }
}
