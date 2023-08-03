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
package com.whaleal.ddt.metadata;

import com.mongodb.lang.Nullable;
import lombok.Getter;
import org.bson.*;
import org.bson.types.*;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Getter
public class BsonTypeMap {
    private static final Map<Class<?>, BsonType> MONGODB_TYPE_MEMBER_MAP = new HashMap<>();

    static {
        MONGODB_TYPE_MEMBER_MAP.put(List.class, BsonType.ARRAY);
        MONGODB_TYPE_MEMBER_MAP.put(Binary.class, BsonType.BINARY);
        MONGODB_TYPE_MEMBER_MAP.put(Boolean.class, BsonType.BOOLEAN);
        MONGODB_TYPE_MEMBER_MAP.put(Date.class, BsonType.DATE_TIME);
        MONGODB_TYPE_MEMBER_MAP.put(BsonDbPointer.class, BsonType.DB_POINTER);
        MONGODB_TYPE_MEMBER_MAP.put(Document.class, BsonType.DOCUMENT);
        MONGODB_TYPE_MEMBER_MAP.put(Double.class, BsonType.DOUBLE);
        MONGODB_TYPE_MEMBER_MAP.put(Integer.class, BsonType.INT32);
        MONGODB_TYPE_MEMBER_MAP.put(Long.class, BsonType.INT64);
        MONGODB_TYPE_MEMBER_MAP.put(Decimal128.class, BsonType.DECIMAL128);
        MONGODB_TYPE_MEMBER_MAP.put(MaxKey.class, BsonType.MAX_KEY);
        MONGODB_TYPE_MEMBER_MAP.put(MinKey.class, BsonType.MIN_KEY);
        MONGODB_TYPE_MEMBER_MAP.put(Code.class, BsonType.JAVASCRIPT);
        MONGODB_TYPE_MEMBER_MAP.put(CodeWithScope.class, BsonType.JAVASCRIPT_WITH_SCOPE);
        MONGODB_TYPE_MEMBER_MAP.put(ObjectId.class, BsonType.OBJECT_ID);
        MONGODB_TYPE_MEMBER_MAP.put(BsonRegularExpression.class, BsonType.REGULAR_EXPRESSION);
        MONGODB_TYPE_MEMBER_MAP.put(String.class, BsonType.STRING);
        MONGODB_TYPE_MEMBER_MAP.put(Symbol.class, BsonType.SYMBOL);
        MONGODB_TYPE_MEMBER_MAP.put(BsonTimestamp.class, BsonType.TIMESTAMP);
        MONGODB_TYPE_MEMBER_MAP.put(BsonUndefined.class, BsonType.UNDEFINED);
    }

    public static Map<Class<?>, BsonType> getMongodbTypeMemberMap() {
        return MONGODB_TYPE_MEMBER_MAP;
    }

    @Nullable
    public BsonType get(Class<?> type) {
        return MONGODB_TYPE_MEMBER_MAP.get(type);
    }

}
