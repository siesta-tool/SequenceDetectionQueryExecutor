package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryStats;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryStatsWrapper;
import com.datalab.siesta.queryprocessor.services.LoadInfo;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Controller
public class UserInterfaceController {
    private final LoadInfo loadInfo;
    private final DBConnector dbConnector;
    private final QueryStats qs;

    @Autowired
    public UserInterfaceController(LoadInfo loadInfo, DBConnector dbConnector, QueryStats qs) {
        this.loadInfo = loadInfo;
        this.dbConnector = dbConnector;
        this.qs = qs;
    }

    @GetMapping("/")
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
        if (m == null) {
            return "fragments/card_content/pattern_detection_cards:: stats_result";
        } else {
            QueryPlan qp = qs.createQueryPlan(qsp, m);
            QueryResponse qrs = qp.execute(qsp);
            model.addAttribute("result", qrs);
            return "fragments/card_content/pattern_detection_cards:: stats_result";
        }
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


    @GetMapping("/ui/{page}")
    public String getUI(@PathVariable String page) {
        return page;
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




}
