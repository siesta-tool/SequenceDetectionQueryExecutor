package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.services.LoadInfo;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import javax.ws.rs.QueryParam;
import java.util.List;
import java.util.Map;

@Controller
public class UserInterfaceController {
    private final LoadInfo loadInfo;
    private final DBConnector dbConnector;

    @Autowired
    public UserInterfaceController(LoadInfo loadInfo, DBConnector dbConnector) {
        this.loadInfo = loadInfo;
        this.dbConnector = dbConnector;
    }

    @GetMapping("/")
    public String home() {
        return "home";
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

    @GetMapping("/ui/query/{logname}")
    public String query(@PathVariable String logname, final Model model) {
        model.addAttribute("logname", logname);
        Metadata m = loadInfo.getMetadata().get(logname);
        model.addAttribute("metadata",m);
        List<String> s = loadInfo.getEventTypes().getOrDefault(logname,null);
        model.addAttribute("eventTypes",s);
        Map<String,Long> l = loadInfo.getEventTypeOccurrences().getOrDefault(logname,null);
        model.addAttribute("eventStats",l);
        model.addAttribute("logname", logname);
        return "main_panel";
    }

    @GetMapping("/ui/{page}")
    public String getUI(@PathVariable String page) {
        return page;
    }



}
