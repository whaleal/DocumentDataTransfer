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
 *
 * The events include operations such as insert, delete, update, replace, and various metadata changes
 * like table creation, index creation, renaming, etc.
 *
 * @param <T> The type of event data this interface handles (e.g., ChangeStreamDocument or other appropriate data type)
 */
public interface ParseEventInterface<T> {

    // Operation constants for data changes
    String INSERT_DATA = "insert";
    String DELETE_DATA = "delete";
    String UPDATE_DATA = "update";
    String REPLACE_DATA = "update";

    // Operation constants for metadata changes
    String INVALIDATE = "invalidate";
    String MODIFY_COLLECTION = "modify";
    String RENAME = "rename";
    String SHARD_COLLECTION = "shardCollection";

    String DROP_TABLE = "drop";
    String CREATE_TABLE = "create";
    String CREATE_INDEX = "createIndexes";
    String DROP_INDEX = "dropIndexes";
    String RENAME_COLLECTION = "renameCollection";
    String COMMIT_INDEX_BUILD = "commitIndexBuild";
    String CONVERT_TO_CAPPED = "convertToCapped";
    String DROP_DATABASE = "dropDatabase";
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
