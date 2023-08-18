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
package com.whaleal.ddt.conection;


import com.alibaba.fastjson2.JSON;
import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;


/**
 * Mongodb链接类
 *
 * @author lhp
 * @time 2021-05-31 13:12:12
 */
@Log4j2
public class BaseMongoDBConnection {
    /**
     * 默认的连接库表
     */
    public static final MongoNamespace DEFAULT_NS = new MongoNamespace("test.test");

    /**
     * 检查MongoDB连接是否可达
     *
     * @param url url
     * @return boolean
     */
    public static boolean checkMonoDBConnection(String dsName, String url) {
        boolean isHaveNS = true;
        MongoClient mongoClient = null;
        try {
            mongoClient = MongoClients.create(url);
            String firstDbName = mongoClient.listDatabaseNames().first();
            if (firstDbName == null) {
                firstDbName = DEFAULT_NS.getDatabaseName();
            }
            String firstTableName = mongoClient.getDatabase(firstDbName).listCollectionNames().first();
            if (firstTableName == null) {
                firstTableName = DEFAULT_NS.getCollectionName();
                isHaveNS = false;
            }
            // 没啥用，防止编译器优化掉代码firstNs
            String ns = new MongoNamespace(firstDbName, firstTableName).getFullName();
            if (DEFAULT_NS.getFullName().equalsIgnoreCase(ns) && !isHaveNS) {
                log.warn("{} this data source does not have any library tables, which may affect MongoT operation", dsName);
            }
        } catch (Exception e) {
            log.error("{} failed to check data source connectivity:{}", dsName, e.getMessage());
            return false;
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
        return true;
    }

    public static String printAndGetURLInfo(String dsName, String url) {
        // 可以定制化配置 防止额外信息输出
        ConnectionString connectionString = new ConnectionString(url);
        Document urlInfo = new Document();
        try {
            urlInfo = Document.parse(JSON.toJSONString(connectionString));
            urlInfo.remove("connectionString");
            urlInfo.remove("credential");
            urlInfo.remove("password");
        } catch (Exception ignored) {
        }

        urlInfo.append("database", connectionString.getDatabase());
        urlInfo.append("hosts", connectionString.getHosts());
        urlInfo.append("username", connectionString.getUsername());

        if (connectionString.getPassword() != null && connectionString.getPassword().length > 0) {
            urlInfo.append("password", connectionString.getPassword()[0] + "*** encryption ***" + connectionString.getPassword()[Math.max(0, connectionString.getPassword().length - 1)]);
        }
        log.info("dsName:{},urlInfo:{}", dsName, urlInfo.toJson());
        return urlInfo.toJson().trim();
    }

    /**
     * 检查MongoDB连接是否可达
     *
     * @param dsName      数据源名称
     * @param mongoClient MongoDB客户端
     * @return boolean
     */
    public static boolean checkMonoDBConnection(String dsName, MongoClient mongoClient) {
        try {
            boolean isHaveNS = true;
            String firstDbName = mongoClient.listDatabaseNames().first();
            if (firstDbName == null) {
                firstDbName = DEFAULT_NS.getDatabaseName();
            }
            String firstTableName = mongoClient.getDatabase(firstDbName).listCollectionNames().first();
            if (firstTableName == null) {
                firstTableName = DEFAULT_NS.getCollectionName();
                isHaveNS = false;
            }
            // 没啥用，防止编译器优化掉代码firstNs
            String ns = new MongoNamespace(firstDbName, firstTableName).getFullName();
            if (DEFAULT_NS.getFullName().equalsIgnoreCase(ns) && !isHaveNS) {
                log.warn("{} this data source does not have any library tables, which may affect MongoT operation", dsName);
            }
        } catch (Exception e) {
            log.error("{} failed to check data source connectivity:{}", e.getMessage(), dsName);
            return false;
        }
        return true;
    }

}
