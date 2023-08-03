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
package com.whaleal.ddt.parse.oplog;

import org.bson.Document;

/**
 * @description:
 * @author: lhp
 * @time: 2021/11/19 2:03 下午
 */

public interface ParseOplogInterface {
    /**
     * 删除表
     */
    String DROP_TABLE = "drop";
    /**
     * 建立表
     */
    String CREATE_TABLE = "create";
    /**
     * 创建索引
     */
    String CREATE_INDEX = "createIndexes";
    /**
     * 删除索引
     */
    String DROP_INDEX = "dropIndexes";
    /**
     * 表重命名
     */
    String RENAME_COLLECTION = "renameCollection";
    /**
     * 索引创建成功标识符
     */
    String COMMIT_INDEX_BUILD = "commitIndexBuild";
    /**
     * 修改表的容量上限
     */
    String CONVERT_TO_CAPPED = "convertToCapped";
    /**
     * 删库
     */
    String DROP_DATABASE = "dropDatabase";
    /**
     * 表结构变更
     */
    String COLLECTION_MOD = "collMod";

    /**
     * parseDropTable 解析删表
     *
     * @param document oplog数据
     * @desc 解析删表
     */
    void parseDropTable(Document document);

    /**
     * parseCreateTable 解析创建表
     *
     * @param document oplog数据
     * @desc 解析创建表
     */
    void parseCreateTable(Document document);

    /**
     * parseRenameTable 解析表重命名
     *
     * @param document oplog数据
     * @desc 解析表重命名
     */
    void parseRenameTable(Document document);

    /**
     * parseCreateIndex 解析建立索引
     *
     * @param document oplog数据
     * @desc 解析删表
     */
    void parseCreateIndex(Document document);

    /**
     * parseDropIndex 解析删除索引
     *
     * @param document oplog数据
     * @desc 解析删除索引
     */
    void parseDropIndex(Document document);

    /**
     * parseInsert 解析插入数据
     *
     * @param document oplog数据
     * @desc 解析插入数据
     */
    void parseInsert(Document document);

    /**
     * parseUpdate 解析更新数据
     *
     * @param document oplog数据
     * @desc 解析更新数据
     */
    void parseUpdate(Document document);

    /**
     * parseDelete 解析删除数据
     *
     * @param document oplog数据
     * @desc 解析删除数据
     */
    void parseDelete(Document document);

    /**
     * parseConvertToCapped 修改表容量上限
     *
     * @param document oplog数据
     * @desc 修改表容量上限
     */
    void parseConvertToCapped(Document document);

    /**
     * parseDropDatabase 删库
     *
     * @param document oplog数据
     * @desc 删库
     */
    void parseDropDatabase(Document document);

    /**
     * parseCollMod 解析表结构修改操作
     *
     * @param document oplog数据
     * @desc 解析表结构修改操作
     */
    void parseCollMod(Document document);

    /**
     * updateUniqueIndexCount 更新表唯一索引个数
     *
     * @param ns ns
     * @desc 更新表唯一索引个数
     */
    void updateUniqueIndexCount(String ns);
}
