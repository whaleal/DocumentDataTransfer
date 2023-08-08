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

package com.whaleal.ddt.util;

import com.mongodb.client.model.*;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * Utility class for converting WriteModel instances to String.
 */
@Log4j2
public class WriteModelUtil {

    /**
     * Converts a WriteModel to a String representation.
     *
     * @param writeModel the WriteModel to convert
     * @return a String representation of the WriteModel
     */
    public static String writeModelToString(WriteModel writeModel) {
        StringBuilder stringBuilder = new StringBuilder();

        try {
            // Append the class name of the WriteModel
            stringBuilder.append("type").append(":").append(writeModel.getClass().getSimpleName()).append(",");

            // Handle different types of WriteModel
            if (writeModel instanceof InsertOneModel) {
                stringBuilder.append(handleInsertOneModel((InsertOneModel<Document>) writeModel));
            } else if (writeModel instanceof ReplaceOneModel) {
                stringBuilder.append(handleReplaceOneModel((ReplaceOneModel<Document>) writeModel));
            } else if (writeModel instanceof DeleteManyModel) {
                stringBuilder.append(handleDeleteManyModel((DeleteManyModel<Document>) writeModel));
            } else if (writeModel instanceof DeleteOneModel) {
                stringBuilder.append(handleDeleteOneModel((DeleteOneModel<Document>) writeModel));
            } else if (writeModel instanceof UpdateOneModel) {
                stringBuilder.append(handleUpdateOneModel((UpdateOneModel<Document>) writeModel));
            } else if (writeModel instanceof UpdateManyModel) {
                stringBuilder.append(handleUpdateManyModel((UpdateManyModel<Document>) writeModel));
            } else {
                // Unknown WriteModel type
                stringBuilder.append("unKnown").append(":").append(writeModel.toString());
            }
        } catch (Exception e) {
            handleException(stringBuilder, writeModel, e);
        }

        return stringBuilder.toString();
    }

    /**
     * Handles an InsertOneModel, returning its document as a JSON string.
     *
     * @param model the InsertOneModel to handle
     * @return a JSON string representation of the document
     */
    private static String handleInsertOneModel(InsertOneModel<Document> model) {
        Document insertDoc = model.getDocument();
        return insertDoc.toJson();
    }

    /**
     * Handles a ReplaceOneModel, returning its filter and replacement as strings.
     *
     * @param model the ReplaceOneModel to handle
     * @return a string representation of the filter and replacement
     */
    private static String handleReplaceOneModel(ReplaceOneModel<Document> model) {
        Bson filter = model.getFilter();
        Document replacement = model.getReplacement();
        return "filter:" + filter.toString() + ",replacement:" + replacement.toJson();
    }

    /**
     * Handles a DeleteManyModel, returning its filter as a string.
     *
     * @param model the DeleteManyModel to handle
     * @return a string representation of the filter
     */
    private static String handleDeleteManyModel(DeleteManyModel<Document> model) {
        Bson deleteManyFilterDoc = model.getFilter();
        return "filter:" + deleteManyFilterDoc.toString();
    }

    /**
     * Handles a DeleteOneModel, returning its filter as a string.
     *
     * @param model the DeleteOneModel to handle
     * @return a string representation of the filter
     */
    private static String handleDeleteOneModel(DeleteOneModel<Document> model) {
        Bson deleteFilterDoc = model.getFilter();
        return "filter:" + deleteFilterDoc.toString();
    }

    /**
     * Handles an UpdateOneModel, returning its filter and update as strings.
     *
     * @param model the UpdateOneModel to handle
     * @return a string representation of the filter and update
     */
    private static String handleUpdateOneModel(UpdateOneModel<Document> model) {
        Bson updateFilterDoc = model.getFilter();
        Bson update = model.getUpdate();
        return "filter:" + updateFilterDoc.toString() + ",update:" + update.toString();
    }

    /**
     * Handles an UpdateManyModel, returning its filter and update as strings.
     *
     * @param model the UpdateManyModel to handle
     * @return a string representation of the filter and update
     */
    private static String handleUpdateManyModel(UpdateManyModel<Document> model) {
        Bson updateFilterDoc = model.getFilter();
        Bson update = model.getUpdate();
        return "filter:" + updateFilterDoc.toString() + ",update:" + update.toString();
    }

    /**
     * Handles any exceptions that occur while converting a WriteModel to a string.
     *
     * @param stringBuilder the StringBuilder to append the exception message to
     * @param writeModel    the WriteModel that caused the exception
     * @param e             the exception that occurred
     */
    private static void handleException(StringBuilder stringBuilder, WriteModel writeModel, Exception e) {
        stringBuilder.append(",");
        try {
            stringBuilder.append(writeModel.toString()).append(",");
        } catch (Exception ignored) {
        }
        stringBuilder.append(e.getMessage());
    }
}
