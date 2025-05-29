package com.datalab.siesta.queryprocessor.model.Queries.QueryTypes;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.ExtractedPairsForPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.Detection.*;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Pattern Detection: Based on the characteristics of the query pattern this class determines which query plan will
 * be executed. If the pattern has groups defined then the QueryPlanPatternDetectionGroups will be executed. If
 * the patterns has the explainability setting on then the QueryPlanWhyNotMatch will be executed. If none of the above is
 * correct then the generic QueryPlanPatternDetection will be executed.
 */
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueryPatternDetection implements Query {

    private final QueryPlanPatternDetection qppd;
    private final QueryPlanPatternDetectionGroups qppdg;
    private final QueryPlanWhyNotMatch planWhyNotMatch;
    private final QueryPlanPatternDetectionSingle qpds;
    private final QueryPlanPatternDetectionAttributes qpda;

    @Autowired
    public QueryPatternDetection(@Qualifier("queryPlanPatternDetection") QueryPlanPatternDetection qppd,
                                 @Qualifier("queryPlanPatternDetectionGroups") QueryPlanPatternDetectionGroups qppdg,
                                 @Qualifier("queryPlanPatternDetectionSingle") QueryPlanPatternDetectionSingle qpds,
                                 @Qualifier("queryPlanPatternDetectionAttributes") QueryPlanPatternDetectionAttributes qpda,
                                 QueryPlanWhyNotMatch planWhyNotMatch) {
        this.qppd = qppd;
        this.qppdg = qppdg;
        this.qpds = qpds;
        this.qpda = qpda;
        this.planWhyNotMatch = planWhyNotMatch;
    }

    /**
     * Determines which query plan will be executed. If the pattern has groups defined then the
     * QueryPlanPatternDetectionGroups will be executed. If the patterns has the explainability setting on then the
     * QueryPlanWhyNotMatch will be executed. If none of the above is
     * correct then the generic QueryPlanPatternDetection will be executed.
     */

    @Override
    public QueryPlan createQueryPlan(QueryWrapper qw, Metadata m) {
        QueryPatternDetectionWrapper qpdw = (QueryPatternDetectionWrapper) qw;

        boolean fromOrTillSet = qpdw.getFrom() != null || qpdw.getTill() != null;
        List<ExtractedPairsForPatternDetection> pairs = qpdw.getPattern().extractPairsForPatternDetection(fromOrTillSet);

        if(pairs.isEmpty() || pairs.get(0).getTruePairs().isEmpty()){ //either is empty or there is no true pairs
            qpds.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            qpds.setMetadata(m);
            return qpds;
        }

        if (qpdw.isWhyNotMatchFlag()) {
            planWhyNotMatch.setMetadata(m);
            planWhyNotMatch.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            return planWhyNotMatch;
        } else if (qpdw.isHasGroups()) {
            qppdg.setMetadata(m);
            qppdg.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            return qppdg;
        }
        else if (qpdw.getAttributes() != null && !qpdw.getAttributes().isEmpty() || (qpdw.getEqualAttributes() != null && !qpdw.getEqualAttributes().isEmpty())) {
            qpda.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            qpda.setAttributes(qpdw.getAttributes());
            qpda.setEqualAttributes(qpdw.getEqualAttributes());
            qpda.setMetadata(m);
            return qpda;
        }
        else {
            qppd.setEventTypesInLog(qpdw.getPattern().getEventTypes());
            qppd.setMetadata(m);
            return qppd;
        }
    }
}
