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
package com.whaleal.ddt.sync.parse.ns;

import com.whaleal.ddt.realtime.common.parse.ns.BaseParseNs;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;


/**
 * @author: lhp
 * @time: 2021/7/21 2:38 下午
 * @desc: 解析document的ns
 */
@Log4j2
public class ParseNs extends BaseParseNs<Document> {

    /**
     * ParseNs 类的构造方法。
     *
     * @param workName         工作名称。
     * @param dbTableWhite     数据库表名白名单的正则表达式。
     * @param dsName           数据源名称。
     * @param maxQueueSizeOfNs 单个 namespace 队列的最大大小。
     */
    public ParseNs(String workName, String dbTableWhite, String dsName, int maxQueueSizeOfNs) {
        super(workName, dbTableWhite, dsName, maxQueueSizeOfNs);
    }


    @Override
    public void execute() {
        log.info("{} oplog parsing ns thread starts running", workName);
        // 当前解析oplog日志的个数
        exe();
    }


    @Override
    public void parseNs(Document document) throws InterruptedException {
        String fullDbTableName = document.get("ns").toString();
        String op = document.get("op").toString();
        boolean isDDL = false;
        // DDL
        if ("c".equals(op)) {
            fullDbTableName = parseDDL(document);
            // 判读ddl的数据操作的ns是否符合表名过滤
            if (fullDbTableName.length() == 0 || !fullDbTableName.matches(dbTableWhite)) {
                return;
            }
            isDDL = true;
        }
        String tableName = fullDbTableName.split("\\.", 2)[1];
        // system.buckets.
        // 5.0以后分桶表 可以存储数据 可以参考system.txt说明
        if (tableName.startsWith("system.") && (!tableName.startsWith("system.buckets."))) {
            return;
        }
        pushQueue(fullDbTableName, document, isDDL);
    }

    @Override
    public void addUpdateUniqueIndexInfo(String ns) {
        // 更新此表的唯一索引情况
        Document updateIndexInfo = new Document();
        updateIndexInfo.put("op", "updateIndexInfo");
        try {
            metadata.getQueueOfNsMap().get(ns).put(updateIndexInfo);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * 解析DDL相关日志的方法，根据不同的DDL操作类型进行解析。
     *
     * @param document oplog中的DDL相关日志。
     * @return 表的完整名称。
     */
    public String parseDDL(Document document) {
        Document o = (Document) document.get("o");
        String fullDbTableName = "";
        if (o.get("create") != null) {
            fullDbTableName = parseCreateTable(document);
        } else if (o.get("drop") != null) {
            fullDbTableName = parseDropTable(document);
        } else if (o.get("createIndexes") != null) {
            fullDbTableName = parseCreateIndex(document);
        } else if (o.get("commitIndexBuild") != null) {
            fullDbTableName = parseCommitIndexBuild(document);
        } else if (o.get("dropIndexes") != null) {
            fullDbTableName = parseDropIndex(document);
        } else if (o.get("renameCollection") != null) {
            fullDbTableName = parseRenameTable(document);
        } else if (o.get("convertToCapped") != null) {
            // Q: 可以加上此功能
            // A: convertToCapped=drop+create
        } else if (o.get("dropDatabase") != null) {
            //此方法 不会用到 删除的语句 会变成删除n个删除表语句
            parseDropDataBase(document);
        } else if (o.get("collMod") != null) {
            return parseCollMod(document);
        }
        return fullDbTableName;
    }

    /**
     * parseDropTable
     *
     * @param document oplog
     * @desc 解析DropTableDocument的ns
     */
    private String parseDropTable(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("drop").toString();
        return dbName + "." + tableName;
    }

    /**
     * parseDropDataBase
     *
     * @param document oplog
     * @desc 解析删库
     */
    private void parseDropDataBase(Document document) {
        // 此方法 不会用到 删除的语句 会变成删除n个删除表语句
    }

    /**
     * parseCreateTable
     *
     * @param document oplog
     * @desc 解析createTableDocument的ns
     */
    private String parseCreateTable(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("create").toString();
        return dbName + "." + tableName;
    }

    /**
     * parseRenameTable
     *
     * @param document oplog
     * @desc 解析RenameTableDocument的ns
     */
    private String parseRenameTable(Document document) {
        Document o = (Document) document.get("o");
        return o.get("renameCollection").toString();
    }

    /**
     * parseCreateIndex
     *
     * @param document oplog
     * @desc 解析CreateIndexDocument的ns
     */
    private String parseCreateIndex(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("createIndexes").toString();
        return dbName + "." + tableName;
    }

    /**
     * parseCommitIndexBuild
     *
     * @param document oplog
     * @desc 解析commitIndexBuild的ns
     */
    private String parseCommitIndexBuild(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("commitIndexBuild").toString();
        return dbName + "." + tableName;
    }

    /**
     * parseDropIndex
     *
     * @param document oplog
     * @desc 解析DropIndexDocument的ns
     */
    private String parseDropIndex(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("dropIndexes").toString();
        return dbName + "." + tableName;
    }

    /**
     * parseCollMod
     *
     * @param document oplog
     * @desc 解析parseCollMod的ns
     */
    private String parseCollMod(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("collMod").toString();
        return dbName + "." + tableName;
    }

}
