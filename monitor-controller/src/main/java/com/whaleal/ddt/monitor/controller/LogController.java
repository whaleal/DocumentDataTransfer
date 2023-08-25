package com.whaleal.ddt.monitor.controller;

import com.whaleal.ddt.monitor.service.LogService;
import com.whaleal.ddt.monitor.util.R;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/log")
public class LogController {

    @Autowired
    private LogService logService;

    @GetMapping("/findLog")
    public R findLog(
            @RequestParam (required = false) String type,
            @RequestParam long startTime,
            @RequestParam long endTime,
            @RequestParam (required = false)String info,
            @RequestParam Integer pageIndex,
            @RequestParam Integer pageSize) {
        return R.ok().put("data", logService.findLog(type, startTime, endTime, info, pageIndex, pageSize));
    }
}
