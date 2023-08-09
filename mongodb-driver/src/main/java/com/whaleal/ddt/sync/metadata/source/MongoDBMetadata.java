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
package com.whaleal.ddt.sync.metadata.source;

import com.alibaba.fastjson2.JSON;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.CreateCollectionOptions;
import com.whaleal.ddt.sync.connection.MongoDBConnection;
import com.whaleal.ddt.util.ParserMongoStructureUtil;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Data
@Log4j2
public class MongoDBMetadata {
    /**
     * 数据源名称
     */
    private final String dsName;
    /**
     * 数据库客户端
     */
    private final MongoClient client;

    public MongoDBMetadata(String dsName) {
        this.dsName = dsName;
        this.client = MongoDBConnection.getMongoClient(dsName);
    }

    /**
     * 获取指定条件下的库表列表
     *
     * @param dbTableWhite 库表白名单正则表达式
     * @return List<String> 匹配条件的库表列表
     */
    public List<String> getNSList(String dbTableWhite) {
        List<String> nsList = new ArrayList<>();
        try {
            // 遍历库列表
            for (String dbName : client.listDatabaseNames()) {
                // 此操作有可能无权限遍历库表信息
                if ("admin".equalsIgnoreCase(dbName) || "local".equalsIgnoreCase(dbName) || "config".equalsIgnoreCase(dbName)) {
                    log.info("不同步库:{}数据", dbName);
                    continue;
                }
                // 遍历表列表
                for (String tableName : client.getDatabase(dbName).listCollectionNames()) {
                    String ns = dbName + "." + tableName;
                    // 顺序不可写法反
                    // system开头的表 不用管。
                    // 5.0的分桶表 也可以用原表进行处理和建立
                    if (ns.matches(dbTableWhite) && (!tableName.startsWith("system."))) {
                        //不同步 表名开头为system.的表
                        nsList.add(ns);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("{}获取NS列表信息失败:{}", dsName, e.getMessage());
        }
        log.info("{} 获取NS:{}", dsName, JSON.toJSONString(nsList));
        return nsList;
    }

    /**
     * 打印数据源中的用户信息
     */
    public void printUserInfo() {
        log.warn("开始打印数据源{}中用户信息", dsName);
        try {
            MongoCursor<Document> cursor = client.getDatabase("admin").getCollection("system.users").find(new BasicDBObject()).cursor();
            StringBuilder userInfoStr = new StringBuilder();
            userInfoStr.append("\n\t[");
            while (cursor.hasNext()) {
                userInfoStr.append(cursor.next().toJson()).append(" , ");
            }
            userInfoStr.append("]\n\t");
            log.warn("{}用户信息:{}", dsName, userInfoStr);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("{}打印用户信息失败:{}", dsName, e.getMessage());
        }
    }

    /**
     * 获取库表的创建选项映射
     *
     * @param dbTableWhite 库表白名单正则表达式
     * @return Map<String, CreateCollectionOptions> 库表与创建选项的映射
     */
    public Map<String, CreateCollectionOptions> getCollectionOptionMap(String dbTableWhite) {
        Map<String, CreateCollectionOptions> collectionOptionMap = new HashMap<>();
        for (String dbName : client.listDatabaseNames()) {
            if ("admin".equalsIgnoreCase(dbName) || "local".equalsIgnoreCase(dbName) || "config".equalsIgnoreCase(dbName)) {
                continue;
            }
            for (Document collectionInfo : client.getDatabase(dbName).listCollections()) {
                String tableName = collectionInfo.get("name").toString();
                String ns = dbName + "." + tableName;
                try {
                    // system.开头的表 可以不管
                    if (ns.matches(dbTableWhite) && !tableName.startsWith("system.")) {
                        // 普通表 和 时序表
                        if (collectionInfo.containsKey("type")
                                && ("collection".equals(collectionInfo.get("type").toString())
                                || "timeseries".equals(collectionInfo.get("type").toString()))) {
                            // 表结构
                            Document options = collectionInfo.get("options", Document.class);
                            if (options != null && !options.isEmpty()) {
                                log.info("{} 获取{}表结构:{}", dbName, ns, collectionInfo.toJson());
                                collectionOptionMap.put(ns, ParserMongoStructureUtil.parseCreateCollectionOption(options));
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("{}获取库表{}表结构失败:{}", dsName, ns, e.getMessage());
                }
            }
        }
        return collectionOptionMap;
    }

    /**
     * 获取视图选项映射
     *
     * @param dbTableWhite 库表白名单正则表达式
     * @return Map<String, Document> 视图与选项的映射
     */
    public Map<String, Document> getViewOptionMap(String dbTableWhite) {
        Map<String, Document> viewOptionMap = new HashMap<>();
        for (String dbName : client.listDatabaseNames()) {
            if ("admin".equalsIgnoreCase(dbName) || "local".equalsIgnoreCase(dbName) || "config".equalsIgnoreCase(dbName)) {
                continue;
            }
            for (Document viewInfo : client.getDatabase(dbName).listCollections()) {
                String tableName = viewInfo.get("name").toString();
                String ns = dbName + "." + tableName;
                if (ns.matches(dbTableWhite) && !tableName.startsWith("system.")) {
                    if (viewInfo.containsKey("type") && "view".equals(viewInfo.get("type").toString())) {
                        log.info("{} 获取{}视图结构:{}", dbName, ns, viewInfo.toJson());
                        viewOptionMap.put(ns, viewInfo);
                    }
                }
            }
        }
        return viewOptionMap;
    }

    /**
     * 获取配置设置列表
     *
     * @return List<Document> 配置设置列表
     */
    public List<Document> getConfigSettingList() {
        List<Document> configSettingList = new ArrayList<>();
        for (Document document : client.getDatabase("config").getCollection("settings").find(new BasicDBObject())) {
            log.info("{} config信息:{}", dsName, document.toJson());
            configSettingList.add(document);
        }
        return configSettingList;
    }

    /**
     * 获取分片集合的拆分列表
     *
     * @param dbTableWhite 库表白名单正则表达式
     * @return List<Document> 分片集合的拆分列表
     */
    public List<Document> getShardCollectionSplit(String dbTableWhite) {
        List<Document> splitList = new ArrayList<>();
        breakFlag:
        for (Document next : client.getDatabase("config").getCollection("chunks").find(new BasicDBObject())) {
            String ns = null;
            try {
                Document min = next.get("min", Document.class);
                Document max = next.get("max", Document.class);
                if (next.containsKey("ns")) {
                    ns = next.getString("ns");
                } else {
                    // mongo5.0+  config.chunksa中 使用uuid作为ns信息
                    Document first = client.getDatabase("config").getCollection("collections").find(new BasicDBObject("uuid", next.get("uuid"))).first();
                    if (first == null) {
                        continue;
                    }
                    ns = first.get("_id").toString();
                }
                if (ns == null) {
                    continue;
                }
                String dbName = ns.split("\\.", 2)[0];
                if ("config".equals(dbName) || "admin".equals(dbName) || "local".equals(dbName)) {
                    continue;
                }
                if (!ns.matches(dbTableWhite)) {
                    continue;
                }
                for (Map.Entry<String, Object> entry : max.entrySet()) {
                    Object value = entry.getValue();
                    if ("MaxKey".equalsIgnoreCase(value.getClass().getSimpleName())) {
                        continue breakFlag;
                    }
                    if ("MinKey".equalsIgnoreCase(value.getClass().getSimpleName())) {
                        continue breakFlag;
                    }
                }
                for (Map.Entry<String, Object> entry : min.entrySet()) {
                    Object value = entry.getValue();
                    if ("MaxKey".equalsIgnoreCase(value.getClass().getSimpleName())) {
                        continue breakFlag;
                    }
                    if ("MinKey".equalsIgnoreCase(value.getClass().getSimpleName())) {
                        continue breakFlag;
                    }
                }
                Document split = new Document("split", ns).append("middle", max);
                log.info("{} 获取shard表数据:{}", dsName, split.toJson());
                splitList.add(split);
            } catch (Exception e) {
                log.error("{}获取shard表{} split信息失败:{}", dsName, ns, e.getMessage());
            }
        }
        return splitList;
    }

    /**
     * 获取分片键列表
     *
     * @param dbTableWhite 库表白名单正则表达式
     * @return List<Document> 分片键列表
     */
    public List<Document> getShardKey(String dbTableWhite) {
        List<Document> shardKeyList = new ArrayList<>();
        MongoCollection<Document> collection = client.getDatabase("config").getCollection("collections");
        for (Document document : collection.find(new BasicDBObject())) {
            try {
                MongoNamespace namespace = new MongoNamespace(document.getString("_id"));
                String ns = namespace.getFullName();
                String dbName = namespace.getDatabaseName();
                if ("config".equals(dbName) || "admin".equals(dbName) || "local".equals(dbName)) {
                    continue;
                }
                if (ns.matches(dbTableWhite) && (!document.getBoolean("dropped", false)) && document.containsKey("key")) {
                    Document tempDoc = new Document();
                    tempDoc.append("shardCollection", ns);
                    tempDoc.append("key", document.get("key", Document.class));
                    tempDoc.append("unique", document.getBoolean("unique"));
                    if (document.containsKey("options")) {
                        tempDoc.append("options", document.get("options"));
                    }
                    log.info("{} 获取分片键数据:{}", dsName, tempDoc.toJson());
                    shardKeyList.add(tempDoc);
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{}获取shardKey信息失败:{}", dsName, e.getMessage());
            }
        }
        return shardKeyList;
    }

    /**
     * 获取分片数据库名称列表
     *
     * @return List<String> 分片数据库名称列表
     */
    public List<String> getShardingDBNameList() {
        List<String> shardingDBNameList = new ArrayList<>();
        for (Document next : client.getDatabase("config").getCollection("databases").find(new BasicDBObject())) {
            try {
                if (next.getBoolean("partitioned", false)) {
                    log.info("{} 获取分片数据库名称:{}", dsName, next.get("_id").toString());
                    shardingDBNameList.add(next.get("_id").toString());
                }
            } catch (Exception e) {
                log.error("{}获取ShardingDBName信息失败:{}", dsName, e.getMessage());
            }
        }
        return shardingDBNameList;
    }

    /**
     * 获取索引列表
     *
     * @param dbTableWhite 库表白名单正则表达式
     * @return Queue<Document> 索引列表
     */
    public BlockingQueue<Document> getIndexList(String dbTableWhite) {
        BlockingQueue<Document> indexQueue = new LinkedBlockingQueue<>();
        for (String ns : getNSList(dbTableWhite)) {
            MongoNamespace mongoNamespace = new MongoNamespace(ns);
            String databaseName = mongoNamespace.getDatabaseName();
            String collectionName = mongoNamespace.getCollectionName();
            for (Document document : client.getDatabase(databaseName).getCollection(collectionName).listIndexes()) {
                // _id过滤掉
                if (!"_id_".equals(document.get("name"))) {
                    document.append("ns", databaseName + "." + collectionName);
                    log.info("{} 获取索引信息:{}", dsName, document.toJson());
                    indexQueue.add(document);
                }
            }
        }
        return indexQueue;
    }

    /**
     * 获取指定命名空间中的文档估计数量
     *
     * @param ns 命名空间
     * @return long 估计的文档数量
     */
    public long estimatedDocumentCount(String ns) {
        MongoNamespace namespace = new MongoNamespace(ns);
        return client.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName()).estimatedDocumentCount();
    }

    public static void main(String[] args) {

    }
}
