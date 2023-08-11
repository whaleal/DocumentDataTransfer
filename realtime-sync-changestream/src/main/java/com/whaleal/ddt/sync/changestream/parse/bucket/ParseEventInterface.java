/*
 * Document Data Transfer - An open-source project licensed under GPL+SSPL
 *
 * Copyright (C) [2023 - present] [Whaleal]
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
package com.whaleal.ddt.sync.changestream.parse.bucket;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

/**
 * This interface defines methods for parsing various database change events.
 * Implementing classes handle different types of events triggered by changes in the database.
 */
public interface ParseEventInterface {
    // Constants representing various event types
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
     * Parses a drop table event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of dropping a table in the database. It takes the
     *       change stream event as a parameter, which provides details about the event.
     */
    void parseDropTable(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Parses a create table event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of creating a new table in the database. It takes the
     *       change stream event as a parameter, which provides details about the event.
     */
    void parseCreateTable(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Parses a table rename event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of renaming a table in the database. It takes the
     *       change stream event as a parameter, which provides details about the event.
     */
    void parseRenameTable(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Parses a create index event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of creating an index in the database. It takes the
     *       change stream event as a parameter, which provides details about the event.
     */
    void parseCreateIndex(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Parses a drop index event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of dropping an index in the database. It takes the
     *       change stream event as a parameter, which provides details about the event.
     */
    void parseDropIndex(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Parses an insert data event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of inserting data into a table in the database. It takes
     *       the change stream event as a parameter, which provides details about the event.
     */
    void parseInsert(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Parses an update data event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of updating data in a table in the database. It takes the
     *       change stream event as a parameter, which provides details about the event.
     */
    void parseUpdate(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Parses a delete data event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of deleting data from a table in the database. It takes
     *       the change stream event as a parameter, which provides details about the event.
     */
    void parseDelete(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Parses a convert to capped table event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of converting a table to a capped table in the database.
     *       It takes the change stream event as a parameter, which provides details about the event.
     */
    void parseConvertToCapped(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Parses a drop database event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of dropping a database. It takes the change stream event
     *       as a parameter, which provides details about the event.
     */
    void parseDropDatabase(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Parses a table structure modification event.
     *
     * @param changeStreamEvent The change stream event containing information about the event.
     * @desc This method handles the event of modifying the structure of a table in the database.
     *       It takes the change stream event as a parameter, which provides details about the event.
     */
    void parseCollMod(ChangeStreamDocument<Document> changeStreamEvent);

    /**
     * Updates the count of unique indexes in a table.
     *
     * @param ns The namespace of the table (database and collection name).
     * @desc This method updates the count of unique indexes in a table specified by the namespace.
     *       It takes the namespace as a parameter, which represents the database and collection name.
     */
    void updateUniqueIndexCount(String ns);
}
