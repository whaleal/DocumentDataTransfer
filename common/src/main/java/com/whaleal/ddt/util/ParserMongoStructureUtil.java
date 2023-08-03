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
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;


public class ParserMongoStructureUtil {

    /**
     * 解析创建索引的option信息
     * <p>
     * 索引明细  主要分为以下几类
     * 通用类型
     * <p>
     * <p>
     * background
     * unique
     * name
     * partialFilterExpression
     * sparse
     * expireAfterSeconds
     * hidden
     * <p>
     * <p>
     * Collation
     * locale: <string>,
     * caseLevel: <boolean>,
     * caseFirst: <string>,
     * strength: <int>,
     * numericOrdering: <boolean>,
     * alternate: <string>,
     * maxVariable: <string>,
     * backwards: <boolean>
     * normalization: <boolean>
     * boolean
     * Text
     * weights
     * default_language
     * language_override
     * textIndexVersion
     * 2dsphere
     * 2dsphereIndexVersion
     * 2d
     * bit
     * min
     * max
     * geoHaystack
     * bucketSize
     * wildcard
     * wildcardProjection
     *
     * @param o option信息
     * @return IndexOptions
     */
    public static IndexOptions parseIndexOptions(Document o) {
        // todo boolean类型的要注意转换格式 true 1 false在java中都为真
        String indexName = o.get("name").toString();
        IndexOptions indexOptions = new IndexOptions().name(indexName);
        if (o.get("unique") != null) {
            indexOptions.unique(Boolean.parseBoolean(o.get("unique").toString()));
        }
        if (o.get("name") != null) {
            indexOptions.name((String) o.get("name"));
        }
        if (o.get("partialFilterExpression") != null) {
            indexOptions.partialFilterExpression((Bson) o.get("partialFilterExpression"));
        }
        if (o.get("sparse") != null) {
            indexOptions.sparse((Boolean) o.get("sparse"));
        }
        if (o.get("expireAfterSeconds") != null) {
            // todo https://www.mongodb.com/docs/v6.0/tutorial/expire-data/
            // 参考 expireAfterSeconds可以被设为NAN
            Long expireAfter = ((Double) Double.parseDouble(o.get("expireAfterSeconds").toString())).longValue();
            //TODO 秒以下会丢失
            indexOptions.expireAfter(expireAfter, TimeUnit.SECONDS);
        }
        if (o.get("hidden") != null) {
            indexOptions.hidden((Boolean) o.get("hidden"));
        }
        if (o.get("storageEngine") != null) {
            //不常用到
            indexOptions.storageEngine((Bson) o.get("storageEngine"));
        }
        //---------deal with Collation
        if (o.get("collation") != null) {
            Document collation = (Document) o.get("collation");
            indexOptions.collation(parseCollation(collation));
        }
        //---------deal with Text
        if (o.get("weights") != null) {
            indexOptions.weights((Bson) o.get("weights"));
        }
        if (o.get("textIndexVersion") != null) {
            indexOptions.textVersion(((Double) Double.parseDouble(o.get("textIndexVersion").toString())).intValue());
        }
        if (o.get("default_language") != null) {
            indexOptions.defaultLanguage((String) o.get("default_language"));
        }
        if (o.get("language_override") != null) {
            indexOptions.languageOverride(o.get("language_override").toString());
        }
        //--------deal with wildcard
        if (o.get("wildcardProjection") != null) {
            indexOptions.wildcardProjection((Bson) o.get("wildcardProjection"));
        }
        //---------deal with geoHaystack
        if (o.get("bucketSize") != null) {
            indexOptions.bucketSize(Double.parseDouble(o.get("bucketSize").toString()));
        }
        //---------deal with  2d
        if (o.get("bits") != null) {
            indexOptions.bits(((Double) Double.parseDouble(o.get("bits").toString())).intValue());
        }
        if (o.get("max") != null) {
            indexOptions.max((Double.parseDouble(o.get("max").toString())));
        }
        if (o.get("min") != null) {
            indexOptions.min((Double.parseDouble(o.get("min").toString())));
        }
        //---------------deal with 2dsphere
        if (o.get("2dsphereIndexVersion") != null) {
            indexOptions.sphereVersion(((Double) Double.parseDouble((o.get("2dsphereIndexVersion").toString()))).intValue());
        }
        return indexOptions;
    }

    /**
     * 解析创建集合的option信息
     *
     * @param options option信息
     * @return CreateCollectionOptions
     */
    public static CreateCollectionOptions parseCreateCollectionOption(Document options) {
        // todo boolean类型的要注意转换格式 true 1 false在java中都为真
        CreateCollectionOptions collectionOptions = new CreateCollectionOptions();
        if (options.get("validator") != null) {
            collectionOptions.validationOptions(new ValidationOptions().validator(options.get("validator", Document.class)));
        }
        // clusteredIndex 识别格式有问题
        if (options.get("expireAfterSeconds") != null) {
            collectionOptions.expireAfter(((Double) Double.parseDouble(options.get("expireAfterSeconds").toString())).longValue(), TimeUnit.SECONDS);
        }
        if (options.get("timeseries") != null) {
            final Document timeseries = options.get("timeseries", Document.class);
            final TimeSeriesOptions timeSeriesOptions = new TimeSeriesOptions(timeseries.getString("timeField"));
            timeSeriesOptions.metaField(timeseries.getString("metaField"));
            if ("HOURS".equalsIgnoreCase(timeseries.get("granularity").toString())) {
                timeSeriesOptions.granularity(TimeSeriesGranularity.HOURS);
            } else if ("MINUTES".equalsIgnoreCase(timeseries.get("granularity").toString())) {
                timeSeriesOptions.granularity(TimeSeriesGranularity.MINUTES);
            } else if ("SECONDS".equalsIgnoreCase(timeseries.get("granularity").toString())) {
                timeSeriesOptions.granularity(TimeSeriesGranularity.SECONDS);
            }
            collectionOptions.timeSeriesOptions(timeSeriesOptions);
        }
        if (options.get("capped") != null && "true".equals(options.get("capped").toString())) {
            collectionOptions.capped(true);
        }
        // 最大内存量
        if (options.get("size") != null) {
            long size = Long.parseLong(options.get("size").toString());
            collectionOptions.sizeInBytes(size);
        }
        // 最大数据条数
        if (options.get("max") != null) {
            long max = Long.parseLong(options.get("max").toString());
            collectionOptions.maxDocuments(max);
        }
        // 整理规则
        if (options.get("locale") != null) {
            Document collation = (Document) options.get("collation");
            collectionOptions.collation(parseCollation(collation));
        }
        return collectionOptions;
    }

    /**
     * 解析创建Collation信息
     *
     * @param collation collation
     * @return Collation
     */
    public static Collation parseCollation(Document collation) {
        // todo boolean类型的要注意转换格式 true 1 false在java中都为真
        Collation.Builder collationBuilder = Collation.builder();
        if (collation.get("locale") != null) {
            collationBuilder.locale(collation.getString("locale"));
        }
        if (collation.get("caseLevel") != null) {
            collationBuilder.caseLevel(collation.getBoolean("caseLevel"));
        }
        if (collation.get("caseFirst") != null) {
            collationBuilder.collationCaseFirst(CollationCaseFirst.fromString(collation.getString("caseFirst")));
        }
        if (collation.get("strength") != null) {
            collationBuilder.collationStrength(CollationStrength.fromInt(collation.getInteger("strength")));
        }
        if (collation.get("numericOrdering") != null) {
            collationBuilder.numericOrdering(collation.getBoolean("numericOrdering"));
        }
        if (collation.get("alternate") != null) {
            collationBuilder.collationAlternate(CollationAlternate.fromString(collation.getString("alternate")));
        }
        if (collation.get("maxVariable") != null) {
            collationBuilder.collationMaxVariable(CollationMaxVariable.fromString(collation.getString("maxVariable")));
        }
        if (collation.get("normalization") != null) {
            collationBuilder.normalization(collation.getBoolean("normalization"));
        }
        if (collation.get("backwards") != null) {
            collationBuilder.backwards(collation.getBoolean("backwards"));
        }
        return collationBuilder.build();
    }
}
