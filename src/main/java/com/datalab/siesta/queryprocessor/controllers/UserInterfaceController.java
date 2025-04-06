package com.datalab.siesta.queryprocessor.controllers;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryExploration;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryPatternDetection;
import com.datalab.siesta.queryprocessor.model.Queries.QueryTypes.QueryStats;
import com.datalab.siesta.queryprocessor.services.LoadInfo;
import com.datalab.siesta.queryprocessor.services.LoadedEventTypes;
import com.datalab.siesta.queryprocessor.services.LoadedMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.List;
import java.util.Map;

@Controller
public class UserInterfaceController {
    private LoadInfo loadInfo;

    @Autowired
    public UserInterfaceController(LoadInfo loadInfo) {
        this.loadInfo = loadInfo;
    }

    @GetMapping("/")
    public String home() {
        return "home";
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
        return "main_panel";
    }

    @GetMapping("/ui/{page}")
    public String getUI(@PathVariable String page) {
        return page;
    }



}
