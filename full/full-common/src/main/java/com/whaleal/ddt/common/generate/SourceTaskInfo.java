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
package com.whaleal.ddt.common.generate;

import com.alibaba.fastjson2.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Log4j2
public class SourceTaskInfo {
    /**
     * range
     */
    private Range range;
    /**
     * dbTableName
     */
    private String ns;
    /**
     * 源数据源名称
     */
    private String sourceDsName;
    /**
     * 目标数据源名称
     */
    private String targetDsName;
    /**
     * 开始时间
     */
    private long startTime;
    /**
     * 结束时间
     */
    private long endTime;

    public SourceTaskInfo(Range range, String ns, String sourceDsName) {
        this.range = range;
        this.ns = ns;
        this.sourceDsName = sourceDsName;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
