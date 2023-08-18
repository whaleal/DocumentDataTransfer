package com.whaleal.ddt.conection.reactive;/*
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


import com.whaleal.ddt.common.Datasource;
import com.whaleal.ddt.conection.BaseMongoDBConnection;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mongodb链接类
 *
 * @author lhp
 * @time 2021-05-31 13:12:12
 */
@Log4j2
public class MongoDBConnectionReactive extends BaseMongoDBConnection {


    /**
     * mongodb的链接
     */
    private static final Map<String, com.mongodb.reactivestreams.client.MongoClient> MONGODB_REACTIVE_CLIENT_MAP = new ConcurrentHashMap<>();

    /**
     * createMonoDbDataBase 创造mongodb客户端
     *
     * @param dsName     数据源名称 数据源名称一般为：'workName'_source
     * @param datasource 数据源配置信息
     * @desc 创造mongodb客户端
     */
    public static synchronized boolean createMonoDBClient(String dsName, Datasource datasource) {
        // 先校验url是否可用
        if (!checkMonoDBConnection(dsName, datasource.getUrl())) {
            log.error("{}数据库不能正常连接", dsName);
            return false;
        }
        if (MONGODB_REACTIVE_CLIENT_MAP.containsKey(dsName)) {
            log.error("{}数据源已被创建", dsName);
            return false;
        } else {
            printAndGetURLInfo(dsName, datasource.getUrl());
            // 创建链接
            com.mongodb.reactivestreams.client.MongoClient mongoClient = com.mongodb.reactivestreams.client.MongoClients.create(datasource.getUrl());
            MONGODB_REACTIVE_CLIENT_MAP.put(dsName, mongoClient);
            return true;
        }
    }

    /**
     * getMongoClient 获取mongodb客户端
     *
     * @param dsName 数据源名称
     * @return MongoClient
     * @desc 获取mongodb客户端
     */
    public static synchronized com.mongodb.reactivestreams.client.MongoClient getMongoClient(String dsName) {
        return MONGODB_REACTIVE_CLIENT_MAP.get(dsName);
    }

    /**
     * close 关闭mongodb客户端
     *
     * @param dsName 数据源名称
     * @desc 关闭mongodb客户端
     */
    public static synchronized void close(String dsName) {
        if (!MONGODB_REACTIVE_CLIENT_MAP.containsKey(dsName)) {
            return;
        }
        MONGODB_REACTIVE_CLIENT_MAP.get(dsName).close();
    }


}
