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
package com.whaleal.ddt.monitor.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author cc
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class LogEntity {
    /**
     * Id
     */
    private String id;

    /**
     * 进程ID
     */
    private String processId;

    /**
     * 日志时间 暂不考虑时区问题
     */
    private long time;
    /**
     * 日志类型
     */
    private String type;

    /**
     * 日志信息
     */
    private String info;

}
