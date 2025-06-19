package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponseGroups;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponsePatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryExploration;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryStats;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryPatternDetectionWrapper;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryStatsWrapper;
import com.datalab.siesta.queryprocessor.services.LoadInfo;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Controller
public class UserInterfaceController {
    private final LoadInfo loadInfo;
    private final DBConnector dbConnector;
    private final QueryStats qs;
    private final QueryPatternDetection qpd;
    private final ObjectMapper objectMapper;

    @Autowired
    public UserInterfaceController(LoadInfo loadInfo, DBConnector dbConnector, QueryStats qs,
                                   QueryPatternDetection qpd, ObjectMapper objectMapper) {
        this.loadInfo = loadInfo;
        this.dbConnector = dbConnector;
        this.qs = qs;
        this.qpd = qpd;
        this.objectMapper = objectMapper;
    }

    @GetMapping({"/"})
    public String home() {
        return "home";
    }

    @GetMapping("/fragments/sidebar-tabs")
    public String getSidebarLinks(@RequestParam(required = false) String currentLogname, Model model) {
        List<String> lognames = loadInfo.getEventTypes().keySet().stream().toList();
        model.addAttribute("lognames", lognames);
        model.addAttribute("currentLogname", currentLogname); // optional: for active highlighting
        return "fragments/sidebar :: lognameTabs";
    }

    @GetMapping("/fragments/metadata-panel")
    public String getMetadata(@RequestParam String logname, Model model) {
        Metadata metadata = dbConnector.getMetadata(logname);
        model.addAttribute("currentLogname", logname);
        model.addAttribute("logname", logname);
        model.addAttribute("metadata", metadata);
        List<String> s = loadInfo.getEventTypes().getOrDefault(logname,null);
        model.addAttribute("eventTypes",s);
        Map<String,Long> l = loadInfo.getEventTypeOccurrences().getOrDefault(logname,null);
        model.addAttribute("eventStats",l);
        return "fragments/query_panels/metadata_panel :: metadata_panel";
    }

    @GetMapping("/fragments/pattern-detection")
    public String getPatternDetection(Model model) {
        List<String> lognames = loadInfo.getEventTypes().keySet().stream().toList();
        model.addAttribute("lognames", lognames);
        return "fragments/query_panels/pattern_detection_panel :: pattern-detection-panel";
    }

    @PostMapping("/fragments/pattern-stats")
    public String getStats(@RequestBody QueryStatsWrapper qsp, Model model) {
        Metadata m = loadInfo.getMetadata().getOrDefault(qsp.getLog_name(),null);
        if(qsp.getPattern().getSize()<2){
            return "fragments/card_content/pattern_detection_cards:: statsSmallSize";
        }
        if (m == null) {
            return "fragments/card_content/pattern_detection_cards:: logNameNotFound";
        } else {
            QueryPlan qp = qs.createQueryPlan(qsp, m);
            QueryResponse qrs = qp.execute(qsp);
            model.addAttribute("result", qrs);
            return "fragments/card_content/pattern_detection_cards:: stats_result";
        }
    }

    @GetMapping("/fragments/constraint-card")
    public String getConstraintCard(@RequestParam int index, Model model) {
        model.addAttribute("index", index);
        return "fragments/card_content/pattern_filters :: constraint_card";
    }



    @GetMapping("/ui/event-graph-data")
    public ResponseEntity<Map<Long,Long>> eventGraphData(
            @RequestParam String logname,
            @RequestParam String graph,
            @RequestParam String event_type) {

        if (graph.equals("instances-per-trace")) {
            Map<Long,Long> data = dbConnector.getInstancesPerTrace(logname,event_type);
            long total = data.values().stream()
                    .mapToLong(Long::longValue)
                    .sum();
            data.put(0L, loadInfo.getEventTypeOccurrences().get(logname).get(event_type)-total);
            return ResponseEntity.ok().body(data);
        }

        return ResponseEntity.notFound().build();
    }


    @GetMapping("/siesta/**")
    public String getUI() {
        return "home";
    }

    @GetMapping("/total_occurrences")
    public ResponseEntity<Long> getTotalOccurrences(@RequestParam(required = false) String logname,
                                                    @RequestParam(required = false) String event_type) {
        Map<String,Long> occurrences = loadInfo.getEventTypeOccurrences().getOrDefault(logname,null);
        if (occurrences == null) {
            return ResponseEntity.notFound().build();
        }
        Long total = occurrences.getOrDefault(event_type,null);
        if (total == null) {
            return ResponseEntity.notFound().build();
        }else{
            return ResponseEntity.ok().body(total);
        }
    }

    @PostMapping(path = "/ui/detection")
    public String patternDetection(@RequestBody QueryPatternDetectionWrapper qpdw,
                                   Model model) throws IOException {
        Metadata m = loadInfo.getMetadata().getOrDefault(qpdw.getLog_name(),null);
        if (m == null) {
            return "error";
        } else {
            QueryPlan qp = qpd.createQueryPlan(qpdw, m);
            QueryResponse qrs = qp.execute(qpdw);
            if(qrs instanceof QueryResponsePatternDetection ) {
                model.addAttribute("results", ((QueryResponsePatternDetection) qrs).getOccurrences());
                return "fragments/card_content/pattern_detection_results::standard_results";
            }else if(qrs instanceof QueryResponseGroups){
                return "fragments/card_content/pattern_detection_results::group_results";
            }else{
                return "error";
            }
        }
    }




}
