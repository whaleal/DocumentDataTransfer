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
package com.whaleal.ddt.metadata;

import com.mongodb.lang.Nullable;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;


@Getter
public class BsonTypeMap {
    private static final Map<String, Integer> MONGODB_TYPE_MEMBER_MAP = new HashMap<>();

    static {
        MONGODB_TYPE_MEMBER_MAP.put("double", 1);
        MONGODB_TYPE_MEMBER_MAP.put("string", 2);
        MONGODB_TYPE_MEMBER_MAP.put("object", 3);
//        MONGODB_TYPE_MEMBER_MAP.put("array", 4);
        MONGODB_TYPE_MEMBER_MAP.put("binData", 5);
        MONGODB_TYPE_MEMBER_MAP.put("undefined", 6);
        MONGODB_TYPE_MEMBER_MAP.put("objectId", 7);
        MONGODB_TYPE_MEMBER_MAP.put("bool", 8);
        MONGODB_TYPE_MEMBER_MAP.put("date", 9);
        MONGODB_TYPE_MEMBER_MAP.put("null", 10);
        MONGODB_TYPE_MEMBER_MAP.put("regex", 11);
        MONGODB_TYPE_MEMBER_MAP.put("dbPointer", 12);
        MONGODB_TYPE_MEMBER_MAP.put("javascript", 13);
        MONGODB_TYPE_MEMBER_MAP.put("symbol", 14);
        MONGODB_TYPE_MEMBER_MAP.put("javascriptWithScope", 15);
        MONGODB_TYPE_MEMBER_MAP.put("int", 16);
        MONGODB_TYPE_MEMBER_MAP.put("timestamp", 17);
        MONGODB_TYPE_MEMBER_MAP.put("long", 18);
        MONGODB_TYPE_MEMBER_MAP.put("decimal", 19);
        MONGODB_TYPE_MEMBER_MAP.put("minKey", -1);
        MONGODB_TYPE_MEMBER_MAP.put("maxKey", 127);
    }

    public static Map<String, Integer> getMongodbTypeMemberMap() {
        return MONGODB_TYPE_MEMBER_MAP;
    }

    @Nullable
    public Integer get(String type) {
        return MONGODB_TYPE_MEMBER_MAP.get(type);
    }

}
