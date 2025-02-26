package com.datalab.siesta.queryprocessor.model.Queries.Wrapper;

/**
 * Metadata Query: it only requires the log database from which the user wants to retrieve the metadata
 */
public class QueryAttributesWrapper extends QueryWrapper {


    public QueryAttributesWrapper(){

    }

    @Override
    public String toString() {
        return "QueryAttributesWrapper{" +
                "log_name='" + log_name + '\'' +
                '}';
    }
}
