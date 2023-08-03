/*
 * MongoT - An open-source project licensed under GPL+SSPL
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
package com.whaleal.ddt.task.generate;


import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.whaleal.ddt.connection.Datasource;
import com.whaleal.ddt.connection.MongoDBConnection;
import com.whaleal.ddt.metadata.BsonTypeMap;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonType;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: lhp
 * @time: 2021/7/16 3:04 下午
 * @desc:
 */
@Log4j2
public class SpliceNsData {

    /**
     * 数据源名称
     */
    private final String dsName;
    /**
     * mongoClient
     */
    private final MongoClient mongoClient;
    /**
     * 一个任务读取大小MB
     * 默认 为32mb
     */
    private int mbSize = 32;

    /**
     * 构造函数，初始化数据源名称和任务读取大小
     *
     * @param dsName 数据源名称
     * @param mbSize 一个任务读取大小（以MB为单位）
     */
    public SpliceNsData(String dsName, int mbSize) {
        this.mbSize = mbSize;
        this.dsName = dsName;
        this.mongoClient = MongoDBConnection.getMongoClient(dsName);
    }

    /**
     * 获取某表中的主键类型的最大和最小值
     *
     * @param ns 库表名
     * @return Map<Integer, Range>
     * 键：主键类型
     * 值：该类型主键的最大和最小值
     * @desc 获取某表中的主键类型的最大和最小值
     */
    public Map<Integer, Range> getIdTypes(String ns) {
        Map<Integer, Range> typeMap = new HashMap<>();
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        BasicDBObject basicDBObject = new BasicDBObject();
        for (Map.Entry<Class<?>, BsonType> next : BsonTypeMap.getMongodbTypeMemberMap().entrySet()) {
            int type = next.getValue().getValue();
            // 过滤不可能为主键数据的类型
//            if (type == 4 || type == 6 || type == 10 || type == 12 || type == -1 || type == 127) {
//                continue;
//            }
            // 当在大表中 查询_id类型为4（array），会出现卡死
            try {
                basicDBObject.append("_id", new Document().append("$type", type));
                Document document = mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).find(basicDBObject)
                        .projection(new BasicDBObject().append("_id", 1)).first();
                // 判断某类型的主键是否有数据
                if (document != null) {
                    log.info("{} {}表存在_id类型为:{}", dsName, ns, next.getKey().toString());
                    Range range = getMaxAndMinIdByNs(ns, type);
                    typeMap.put(type, range);
                }
            } catch (Exception e) {
                log.error("{} an error occurred when splitting the {} table task, and the error message was reported:{}", dsName, ns, e.getMessage());
            }
        }
        return typeMap;
    }

    /**
     * 获取某类型主键的最大和最小值
     *
     * @param ns   库表名
     * @param type 数据类型
     * @return Range
     * 某类型主键的最大和最小值
     * @desc 获取某类型主键的最大和最小值
     */
    public Range getMaxAndMinIdByNs(String ns, int type) {
        Object maxId = Integer.MAX_VALUE;
        Object minId = Integer.MIN_VALUE;
        boolean isSuccessComputeMaxAndMin = false;
        // 循环三次查询
        for (int index = 1; index <= 3; index++) {
            try {
                MongoNamespace mongoNamespace = new MongoNamespace(ns);
                BasicDBObject condition = new BasicDBObject();
                condition.append("_id", new Document().append("$type", type));
                BasicDBObject sort = new BasicDBObject();
                sort.append("_id", -1);
                Document maxDocument = mongoClient.getDatabase(mongoNamespace.getDatabaseName())
                        .getCollection(mongoNamespace.getCollectionName()).find(condition)
                        .sort(sort).first();
                sort.append("_id", 1);
                Document minDocument = mongoClient.getDatabase(mongoNamespace.getDatabaseName())
                        .getCollection(mongoNamespace.getCollectionName()).find(condition)
                        .sort(sort).first();
                maxId = maxDocument.get("_id");
                minId = minDocument.get("_id");
                isSuccessComputeMaxAndMin = true;
                // 此某类型数据range的范围
            } catch (Exception e) {
                log.error("{} an error occurred when calculating the maximum and minimum values of [{}] table _id, and an error message was reported:{}", dsName, ns, e.getMessage());
                isSuccessComputeMaxAndMin = false;
            }
            if (isSuccessComputeMaxAndMin) {
                break;
            } else {
                if (index == 3) {
                    log.error("{} an error occurred when calculating the maximum and minimum values of [{}] table _id, and the error message was reported: Failed to read three times, please check the source URL and permissions", dsName, ns);
                }
            }
        }
        Range range = new Range();
        range.setColumnName("_id");
        range.setMaxValue(maxId);
        range.setMinValue(minId);
        return range;
    }

    /**
     * 切分数据，每分数据最大长度为50w
     *
     * @param ns           库表名
     * @param rangeOfTable 表范围range
     * @param type         数据类型
     * @param rangeSize    切分数据的最大长度
     * @return Range
     * 某个区间的range
     * @desc 切分数据，每分数据最大长度为50w
     */
    public Range splitRange(String ns, Range rangeOfTable, int type, int rangeSize) {
        Range range = new Range();
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        BasicDBObject condition = new BasicDBObject();
        // 不要紧在where条件中单独添加type的查询
        condition.append("_id", new Document("$gte", rangeOfTable.getMinValue()));
        Document document = null;
        // 循环三次查询
        boolean isSuccessComputeMaxAndMin = false;
        for (int index = 1; index <= 3; index++) {
            try {
                document = mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).
                        find(condition).sort(new Document("_id", 1)).projection(new Document("_id", 1)).skip(rangeSize).first();
                isSuccessComputeMaxAndMin = true;
            } catch (Exception e) {
                log.error("{} [{}] condition:[{}],error message was reported:{}", dsName, ns, condition.toJson(), e.getMessage());
                isSuccessComputeMaxAndMin = false;
            }
            if (isSuccessComputeMaxAndMin) {
                break;
            } else {
                if (index == 3) {
                    log.error("{} [{}] condition:[{}],,error message was reported::Failed to split three times", dsName, ns, condition.toJson());
                }
            }
        }
        // 如果当前minId的后xxw条的_id字段为空，说明达到该类型数据的最大值
        if (document != null) {
            Object maxIdRTemp = document.get("_id");
            range.setMinValue(rangeOfTable.getMinValue());
            range.setMaxValue(maxIdRTemp);
            range.setNs(ns);
            rangeOfTable.setMinValue(maxIdRTemp);
        } else {
            range.setMinValue(rangeOfTable.getMinValue());
            range.setMaxValue(rangeOfTable.getMaxValue());
            range.setNs(ns);
            range.setMax(true);
            //要修改总的rangeOfTable的范围
            rangeOfTable.setMinValue(null);
        }
        range.setNs(ns);
        range.setRangeSize(rangeSize);
        return range;
    }

    /**
     * 估算库表的文档数量
     *
     * @param ns 库表名
     * @return long
     * 估算的文档数量
     */
    public long estimateRangeSize(String ns) {
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        return mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).estimatedDocumentCount();
    }

    /**
     * 计算每次查询的批处理大小
     *
     * @param ns 库表名
     * @return int
     * 计算得到的批处理大小
     */
    public int computeBatchSize(String ns) {
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        // 查询改表的collStats
        Document collStats = mongoClient.getDatabase(mongoNamespace.getDatabaseName()).runCommand(new Document("collStats", mongoNamespace.getCollectionName()));
        collStats.remove("wiredTiger");
        log.info("{} ns集合状态信息:{}", dsName, collStats.toJson());
        // 可以任务没有数据
        if (!collStats.containsKey("avgObjSize")) {
            return 10240;
        }
        // 每条数据的byte数
        long avgObjSize = Long.parseLong(collStats.get("avgObjSize").toString());
        try {
            // 最大一批数据 一百万一批数据
            return Math.round(Math.min(mbSize * 1024L * 1024L / (avgObjSize + 0.0F), 999)) + 1;
        } catch (Exception e) {
            return 10240;
        }
    }

    /**
     * 获取库表的范围列表
     *
     * @param ns 库表名
     * @return List<Range>
     * 库表的范围列表
     */
    public List<Range> getRangeList(String ns) {
        List<Range> rangeList = new ArrayList<>();
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        long count = mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).estimatedDocumentCount();
        int batchSize = computeBatchSize(ns);
        log.info("{} ns中预估数据量:{},预计每批数据{}条", dsName, count, batchSize);
        if (count > 0) {
            Map<Integer, Range> map = getIdTypes(ns);
            for (Map.Entry<Integer, Range> next : map.entrySet()) {
                Range rangeOfTable = next.getValue();
                while (rangeOfTable.getMinValue() != null) {
                    Range range = splitRange(ns, rangeOfTable, next.getKey(), batchSize);
                    rangeList.add(range);
                }
            }
        }
        return rangeList;
    }


    public static void main(String[] args) {
        MongoDBConnection.createMonoDBClient("test", new Datasource("mongodb://192.168.12.200:24578"));
        SpliceNsData spliceNsData = new SpliceNsData("test", 16);
        final List<Range> rangeList = spliceNsData.getRangeList("doc.lhp6");
        for (Range range : rangeList) {
            System.out.println(range.toString());
        }
    }
}
