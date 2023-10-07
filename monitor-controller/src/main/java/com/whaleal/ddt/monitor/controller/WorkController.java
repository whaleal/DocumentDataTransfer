/*
 * Document Data Transfer - An open-source project licensed under GPL+SSPL
 *
 * Copyright (C) [2023 - present ] [Whaleal]
 *
 * This program is free software; you can redistribute it and/or modify it under the terms
 * of the GNU General Public License and Server Side Public License (SSPL) as published by
 * the Free Software Foundation; either version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License and SSPL for more details.
 *
 * For more information, visit the official website: [www.whaleal.com]
 */
package com.whaleal.ddt.monitor.controller;

import com.whaleal.ddt.monitor.service.MonitorDataService;
import com.whaleal.ddt.monitor.service.WorkService;
import com.whaleal.ddt.monitor.util.R;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/work")
public class WorkController {

    @Autowired
    private WorkService workService;
    @Autowired
    private MonitorDataService monitorDataService;

    /**
     * 获取工作信息列表
     *
     * @param workName 配置信息的任务名称
     * @return 执行结果，不需要分页
     */
    @GetMapping("/getWorkInfoList")
    public R getWorkInfoList(@RequestParam(required = false, defaultValue = "") String workName) {
        return R.ok().put("data", workService.getWorkInfoList(workName));
    }

    /**
     * 获取特定工作信息
     *
     * @param workName 工作名称
     * @return 执行结果
     */
    @GetMapping("/getWorkInfo/{workName}")
    public R getWorkInfo(@PathVariable("workName") String workName) {
        return R.ok().put("data", workService.getWorkInfo(workName));
    }

    /**
     * 获取工作监控信息
     *
     * @param workName  工作名称
     * @param startTime 监控数据起始时间
     * @param endTime   监控数据结束时间
     * @param type      监控数据类型
     * @return 执行结果
     */
    @GetMapping("/getWorkMonitor/{workName}")
    public R getWorkMonitor(@PathVariable("workName") String workName,
                            @RequestParam("startTime") long startTime,
                            @RequestParam("endTime") long endTime,
                            @RequestParam("type") String type) {

        R r = R.ok();
        Map<Object, Object> workInfo = workService.getWorkInfo(workName);
        if (workInfo == null) {
            return r;
        }
        if (Long.parseLong(workInfo.get("startTime").toString()) > startTime) {
            startTime = Long.parseLong(workInfo.get("startTime").toString());
        }
        if (Long.parseLong(workInfo.get("endTime").toString()) < endTime) {
            endTime = Long.parseLong(workInfo.get("endTime").toString());
        }
        r.putAll(monitorDataService.getWorkMonitor(workName, startTime, endTime, type));
        return r;
    }
}
