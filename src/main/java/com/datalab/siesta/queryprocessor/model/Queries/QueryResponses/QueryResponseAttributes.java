package com.datalab.siesta.queryprocessor.model.Queries.QueryResponses;

import java.util.List;

public class QueryResponseAttributes implements QueryResponse{

    List<String> attributes;

    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    public QueryResponseAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    public QueryResponseAttributes() {

    }
}
