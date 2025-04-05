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

@Controller
public class UserInterfaceController {
    private final LoadedMetadata allMetadata;
    private final LoadedEventTypes loadedEventTypes;

    @Autowired
    public UserInterfaceController(LoadedMetadata allMetadata, LoadedEventTypes loadedEventTypes) {
        this.allMetadata = allMetadata;
        this.loadedEventTypes = loadedEventTypes;
    }

    @GetMapping("/ui/query/{logname}")
    public String query(@PathVariable String logname, final Model model) {
        model.addAttribute("logname", logname);
        Metadata m = allMetadata.getMetadata(logname);
        model.addAttribute("metadata",m);
        List<String> s = loadedEventTypes.getEventTypes().getOrDefault(logname,null);
        model.addAttribute("eventTypes",s);
        return "main_panel";
    }

    @GetMapping("/ui/{page}")
    public String getUI(@PathVariable String page) {
        return page;
    }



}
