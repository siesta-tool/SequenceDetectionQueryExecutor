package com.datalab.siesta.queryprocessor.model.Queries.QueryPlans;

import com.datalab.siesta.queryprocessor.model.DBModel.Count;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Events.EventPair;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseAttributes;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseStats;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryAttributesWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryStatsWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.context.annotation.RequestScope;

import java.util.List;
import java.util.Set;

@Component
@RequestScope
public class QueryPlanAttributes implements QueryPlan{

    private DBConnector dbConnector;

    private Metadata metadata;


    public QueryPlanAttributes() {
    }

    @Autowired
    public QueryPlanAttributes(DBConnector dbConnector) {
        this.dbConnector=dbConnector;
    }


    public QueryResponse execute(QueryWrapper qa) {
        QueryAttributesWrapper qaw = (QueryAttributesWrapper)qa;
        List<String> attributes = dbConnector.getAttributes(qaw.getLog_name());
        return new QueryResponseAttributes(attributes);
    }

    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata=metadata;
    }
}
