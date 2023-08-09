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
package com.whaleal.ddt.common;



import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;



/**
 * @author: lhp
 * @time: 2021/7/19 5:02 下午
 * @desc 数据源类
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@Log4j2
public class Datasource {
    /**
     * 数据源名称
     */
    private String name;
    /**
     * 数据库实例名
     */
    private String dbAuth;
    /**
     * 用户名
     */
    private String username;
    /**
     * 密码
     */
    private String password;
    /**
     * 数据源url
     */
    private String url;
    /**
     * 数据源ip
     */
    private String ip;
    /**
     * 数据源端口
     */
    private String port;
    /**
     * 状态：1启用 2禁用
     */
    private Boolean status;
    /**
     * 创建时间
     */
    private long createDate;
    /**
     * 更新时间
     */
    private long updateDate;
    /**
     * 数据源备注
     */
    private String remark;
    /**
     * 数据源选项
     */
    private String dsOption;

    public Datasource(String url) {
        this.url = url;
    }


}
