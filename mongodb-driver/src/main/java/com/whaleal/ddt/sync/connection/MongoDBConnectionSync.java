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
package com.whaleal.ddt.sync.connection;

import com.alibaba.fastjson2.JSON;
import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.whaleal.ddt.common.Datasource;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mongodb链接类
 *
 * @author lhp
 * @time 2021-05-31 13:12:12
 */
@Log4j2
public class MongoDBConnectionSync {
    /**
     * 默认的连接库表
     */
    public static final MongoNamespace DEFAULT_NS = new MongoNamespace("test.test");
    /**
     * mongodb的链接
     */
    private static final Map<String, MongoClient> MONGODB_CLIENT_MAP = new ConcurrentHashMap<>();

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
            log.error("{} the database cannot be connected properly", dsName);
            return false;
        }
        if (MONGODB_CLIENT_MAP.containsKey(dsName)) {
            log.error("{} the data source has been created", dsName);
            return false;
        } else {
            printAndGetURLInfo(dsName, datasource.getUrl());
            // 创建链接
            MongoClient mongoClient = MongoClients.create(datasource.getUrl());
            // 再次检测url是否可达
            if (!checkMonoDBConnection(dsName, mongoClient)) {
                log.error("{} the database cannot be connected properly", dsName);
                return false;
            }
            MONGODB_CLIENT_MAP.put(dsName, mongoClient);
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
    public static synchronized MongoClient getMongoClient(String dsName) {
        return MONGODB_CLIENT_MAP.get(dsName);
    }

    /**
     * close 关闭mongodb客户端
     *
     * @param dsName 数据源名称
     * @desc 关闭mongodb客户端
     */
    public static synchronized void close(String dsName) {
        if (!MONGODB_CLIENT_MAP.containsKey(dsName)) {
            return;
        }
        MONGODB_CLIENT_MAP.get(dsName).close();
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

    /**
     * 获取MongoDB数据源版本
     *
     * @param dsName 数据源名称
     * @return 数据源版本号
     */
    public static String getVersion(String dsName) {
        String version = "4.4";
        try {
            MongoClient client = MongoDBConnectionSync.getMongoClient(dsName);
            String firstDbName = client.listDatabaseNames().first();
            if (firstDbName == null) {
                firstDbName = "test";
            }
            Document fcv = client.getDatabase(firstDbName).runCommand(new Document().append("buildInfo", 1));
            if (fcv.containsKey("version")) {
                log.info("{} version:{}", dsName, fcv.get("version").toString());
                version = fcv.get("version").toString();
            }
        } catch (Exception e) {
            log.error("{} an exception occurred when getting the data source version information,msg:{}", dsName, e.getMessage());
        }
        log.info("{} the data source version information is:{}", dsName, version);
        // 默认 使用4.4
        return version;
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

    public static void main(String[] args) {
        printAndGetURLInfo("1", "mongodb://username:password@host1:1,host2:2,host3:3/mydatabase?replicaSet=myReplSet&authSource=admin&maxIdleTimeMS=60000&readConcern=majority&retryWrites=true");
    }
}
