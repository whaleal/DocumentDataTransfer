/*
 * Document Data Transfer - An open-source project licensed under GPL+SSPL
 *
 * Copyright (C) 2023 - present Whaleal
 *
 * This program is free software; you can redistribute it and/or modify it under the terms
 * of the GNU General Public License and Server Side Public License (SSPL) as published by
 * the Free Software Foundation; either version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License and SSPL for more details.
 *
 * For more information, visit the official website: www.whaleal.com
 */

package com.whaleal.ddt.realtime.common.distribute.bucket;


/**
 * Interface for parsing and handling different types of parse events related to data changes
 * in a document-oriented data transfer system.
 * <p>
 * The events include operations such as insert, delete, update, replace, and various metadata changes
 * like table creation, index creation, renaming, etc.
 *
 * @param <T> The type of event data this interface handles (e.g., ChangeStreamDocument or other appropriate data type)
 * @author liheping
 */
public interface ParseEventInterface<T> {
    /**
     * INSERT_DATA: 代表插入数据的操作
     */
    String INSERT_DATA = "insert";

    /**
     * DELETE_DATA: 代表删除数据的操作
     */
    String DELETE_DATA = "delete";

    /**
     * UPDATE_DATA: 代表更新数据的操作
     */
    String UPDATE_DATA = "update";

    /**
     * REPLACE_DATA: 代表替换数据的操作
     */
    String REPLACE_DATA = "replace";

    /**
     * INVALIDATE: 代表无效化操作，通常在MongoDB的ChangeStream中使用，表示某些更改可能无法被观察到
     */
    String INVALIDATE = "invalidate";

    /**
     * MODIFY_COLLECTION: 代表修改集合的操作
     */
    String MODIFY_COLLECTION = "modify";

    /**
     * RENAME: 代表重命名操作
     */
    String RENAME = "rename";

    /**
     * SHARD_COLLECTION: 代表分片集合的操作
     */
    String SHARD_COLLECTION = "shardCollection";

    /**
     * DROP_TABLE: 代表删除表（或集合）的操作
     */
    String DROP_TABLE = "drop";

    /**
     * CREATE_TABLE: 代表创建表（或集合）的操作
     */
    String CREATE_TABLE = "create";

    /**
     * CREATE_INDEX: 代表创建索引的操作
     */
    String CREATE_INDEX = "createIndexes";

    /**
     * DROP_INDEX: 代表删除索引的操作
     */
    String DROP_INDEX = "dropIndexes";

    /**
     * RENAME_COLLECTION: 代表重命名集合的操作
     */
    String RENAME_COLLECTION = "renameCollection";

    /**
     * COMMIT_INDEX_BUILD: 代表提交索引构建的操作，在构建索引过程中使用
     */
    String COMMIT_INDEX_BUILD = "commitIndexBuild";

    /**
     * CONVERT_TO_CAPPED: 代表将普通集合转换为定容集合的操作
     */
    String CONVERT_TO_CAPPED = "convertToCapped";

    /**
     * DROP_DATABASE: 代表删除数据库的操作
     */
    String DROP_DATABASE = "dropDatabase";

    /**
     * COLLECTION_MOD: 代表集合修改的操作
     */
    String COLLECTION_MOD = "collMod";


    /**
     * Parse and handle the event of dropping a table.
     *
     * @param event Event data for the drop table operation
     */
    void parseDropTable(T event);

    /**
     * Parse and handle the event of creating a table.
     *
     * @param event Event data for the create table operation
     */
    void parseCreateTable(T event);

    /**
     * Parse and handle the event of renaming a table.
     *
     * @param event Event data for the rename table operation
     */
    void parseRenameTable(T event);

    /**
     * Parse and handle the event of creating an index.
     *
     * @param event Event data for the create index operation
     */
    void parseCreateIndex(T event);

    /**
     * Parse and handle the event of dropping an index.
     *
     * @param event Event data for the drop index operation
     */
    void parseDropIndex(T event);

    /**
     * Parse and handle the event of inserting data.
     *
     * @param event Event data for the insert operation
     */
    void parseInsert(T event);

    /**
     * Parse and handle the event of updating data.
     *
     * @param event Event data for the update operation
     */
    void parseUpdate(T event);

    /**
     * Parse and handle the event of deleting data.
     *
     * @param event Event data for the delete operation
     */
    void parseDelete(T event);

    /**
     * Parse and handle the event of converting a collection to a capped collection.
     *
     * @param event Event data for the convert to capped operation
     */
    void parseConvertToCapped(T event);

    /**
     * Parse and handle the event of dropping a database.
     *
     * @param event Event data for the drop database operation
     */
    void parseDropDatabase(T event);

    /**
     * Parse and handle the event of modifying the structure of a collection.
     *
     * @param event Event data for the collection modification operation
     */
    void parseCollMod(T event);

    /**
     * Handle the modification of a collection (generic handler).
     *
     * @param event Event data for the collection modification operation
     */
    void modifyCollection(T event);

    /**
     * Handle the sharding of a collection (generic handler).
     *
     * @param event Event data for the shard collection operation
     */
    void shardCollection(T event);

    /**
     * Parse and handle the event of replacing data.
     *
     * @param event Event data for the replace operation
     */
    void parseReplace(T event);

    /**
     * Update the count of unique indexes for a table.
     *
     * @param ns Namespace of the table
     */
    void updateUniqueIndexCount(String ns);
}
