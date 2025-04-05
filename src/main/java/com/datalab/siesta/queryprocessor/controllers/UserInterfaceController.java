package com.datalab.siesta.queryprocessor.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@Controller
public class UserInterfaceController {


    @GetMapping("/ui/{page}")
    public String getUI(@PathVariable String page) {
        return page;
    }

}
