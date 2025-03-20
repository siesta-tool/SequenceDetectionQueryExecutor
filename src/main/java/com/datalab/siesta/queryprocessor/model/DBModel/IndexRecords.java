package com.datalab.siesta.queryprocessor.model.DBModel;

import com.clearspring.analytics.util.Lists;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A grouping of the IndexPairs based on the et-types
 * @see com.datalab.siesta.queryprocessor.model.DBModel.IndexPair
 */
public class IndexRecords {

    private Map<EventTypes, List<IndexPair>> records;

    public IndexRecords(Map<EventTypes,List<IndexPair>> results) {
        this.records = results;
    }

    public Map<EventTypes, List<IndexPair>> getRecords() {
        return records;
    }

    public void setRecords(Map<EventTypes, List<IndexPair>> records) {
        this.records = records;
    }
}
