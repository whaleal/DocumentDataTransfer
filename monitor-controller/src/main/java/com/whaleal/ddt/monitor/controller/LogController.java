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
