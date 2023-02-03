package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.model.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryStats;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryMetadataWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryStatsWrapper;
import com.datalab.siesta.queryprocessor.services.LoadedEventTypes;
import com.datalab.siesta.queryprocessor.services.LoadedMetadata;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/")
public class QueryResponseController {

    @Autowired
    private DBConnector dbConnector;

    @Autowired
    private LoadedMetadata allMetadata;

    @Autowired
    private QueryStats qs;

    @Autowired
    private QueryPatternDetection qpd;

//    @Autowired
//    private LoadedEventTypes e;


    @RequestMapping(path = "/metadata",method = RequestMethod.GET)
    public ResponseEntity<MappingJacksonValue> getMetadata(@RequestBody QueryMetadataWrapper qmw) {
        String logname = qmw.getLog_name();
        Metadata m = allMetadata.getMetadata(logname);
        if(m == null){
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }else{
            MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(m);
            return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
        }
    }

    @RequestMapping(path = "/stats",method = RequestMethod.GET)
    public ResponseEntity<MappingJacksonValue> getStats(@RequestBody QueryStatsWrapper qsp){
        Metadata m = allMetadata.getMetadata(qsp.getLog_name());
        if(m == null){
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }else{
            QueryPlan qp = qs.createQueryPlan(qsp,m);
            QueryResponse qrs = qp.execute(qsp);
            MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(qrs);
            return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
        }
    }

    @RequestMapping(path = "/detection",method = RequestMethod.GET)
    public ResponseEntity<MappingJacksonValue> patternDetection(@RequestBody QueryPatternDetectionWrapper qpdw){
        Metadata m = allMetadata.getMetadata(qpdw.getLog_name());
        if(m == null){
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }else{
            QueryPlan qp = qpd.createQueryPlan(qpdw,m);
            QueryResponse qrs = qp.execute(qpdw);
            MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(qrs);
            return new ResponseEntity<>(mappingJacksonValue, HttpStatus.OK);
        }
    }
}
