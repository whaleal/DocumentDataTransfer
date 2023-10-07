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

import java.util.List;
import java.util.Map;

public interface WorkService {

    /**
     * 插入或更新工作信息
     *
     * @param workName  工作名称
     * @param workInfo  工作信息
     */
    void upsertWorkInfo(String workName, Map<Object, Object> workInfo);

    /**
     * 获取特定工作信息
     *
     * @param workName  工作名称
     * @return 工作信息
     */
    Map<Object, Object> getWorkInfo(String workName);

    /**
     * 获取工作信息列表
     *
     * @param workName  工作名称
     * @return 工作信息列表
     */
    List<Map<Object, Object>> getWorkInfoList(String workName);
}
