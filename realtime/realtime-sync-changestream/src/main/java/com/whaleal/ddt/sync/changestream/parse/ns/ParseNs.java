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
package com.whaleal.ddt.sync.changestream.parse.ns;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.whaleal.ddt.realtime.common.parse.ns.BaseParseNs;
import lombok.extern.log4j.Log4j2;
import org.bson.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * 命名空间解析类，用于解析ChangeStreamDocument的命名空间。
 *
 * @param <T> ChangeStreamDocument类型的泛型参数。
 * @author liheping
 */
@Log4j2
public class ParseNs extends BaseParseNs<ChangeStreamDocument<Document>> {
    /**
     * ChangeStreamDocument中DDL事件操作类型
     */
    private static final Set<String> ddlOperations = new HashSet<>(Arrays.asList(
            "create", "createIndexes", "drop", "dropDatabase",
            "dropIndexes", "rename", "modify", "shardCollection"
    ));

    /**
     * 构造函数，用于初始化命名空间解析类。
     *
     * @param workName         工作/任务的名称。
     * @param dbTableWhite     数据库表白名单。
     * @param dsName           数据源的名称。
     * @param maxQueueSizeOfNs 命名空间最大队列大小。
     */
    public ParseNs(String workName, String dbTableWhite, String dsName, int maxQueueSizeOfNs, Set<String> ddlSet) {
        super(workName, dbTableWhite, dsName, maxQueueSizeOfNs, ddlSet);
    }

    @Override
    public void execute() {
        log.info("{} changeStream parsing ns thread starts running", workName);
        // 当前解析oplog日志的个数
        exe();
    }


    @Override
    public void parseNs(ChangeStreamDocument<Document> changeStreamEvent) throws InterruptedException {
        // getFullName 已在上级进行判断了，不会出现空指针
        String fullDbTableName = changeStreamEvent.getNamespace().getFullName();
        String op = changeStreamEvent.getOperationTypeString();
        boolean isDDL = ddlOperations.contains(op) && ddlSet.contains(op);
        // todo 这一款需要修改 调研日志
        // system.buckets.
        // 5.0以后分桶表 可以存储数据 可以参考system.txt说明
        pushQueue(fullDbTableName, changeStreamEvent, isDDL);
    }

    @Override
    public void addUpdateUniqueIndexInfo(String ns) {
        // 更新此表的唯一索引情况
        ChangeStreamDocument<Document> changeStreamEvent =
                new ChangeStreamDocument<Document>("updateIndexInfo", new BsonDocument(), new BsonDocument(),
                        new BsonDocument(), new Document(), new Document(), new BsonDocument(), new BsonTimestamp(), new UpdateDescription(new ArrayList<>(), new BsonDocument()), new BsonInt64(0),
                        new BsonDocument(), new BsonDateTime(0L), new BsonDocument());
        try {
            metadata.getQueueOfNsMap().get(ns).put(changeStreamEvent);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
