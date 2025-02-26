package com.datalab.siesta.queryprocessor.model.Queries.QueryTypes;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlanAttributes;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QueryAttributes implements Query{

    @Autowired
    private QueryPlanAttributes qp;


    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        return qp;
    }
}
