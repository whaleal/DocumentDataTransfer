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
package com.whaleal.ddt.monitor.service;


import com.whaleal.ddt.monitor.model.LogEntity;

import java.util.List;

/**
 * @author cc
 */
public interface LogService {


    /**
     * 根据条件查询日志
     *
     * @param type      同步类型
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @param info      日志内容
     * @param pageIndex 第几页
     * @param pageSize  每页大小
     * @return 日志数据
     */
    List<LogEntity> findLog(String type, long startTime, long endTime, String info, Integer pageIndex, Integer pageSize);



    void saveLog(LogEntity logEntity);
}
