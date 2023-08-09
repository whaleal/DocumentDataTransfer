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
package com.whaleal.ddt.sync.parse.oplog;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.Map;
import java.util.Set;

/**
 * 多线程操作解析Oplog。对应MongoDB版本5.x和6.x，为V3版本。
 */
@Log4j2
public class BucketOplogForGteMongoDB5 extends BucketOplog {

    public BucketOplogForGteMongoDB5(String workName, String dsName, int maxBucketNum, Set<String> ddlList, int ddlWait) {
        super(workName, dsName, maxBucketNum, ddlList, ddlWait);
    }

    /**
     * 解析更新数据
     *
     * @param document oplog数据
     * @desc 解析更新数据
     */
    @Override
    public void parseUpdate(Document document) {
        // Q: 可读性不强 待梳理
        // A: 可以单独进行培训讲解，代码走查
        // 获取_id字段的值
        String _id = ((Document) document.get("o2")).get("_id").toString();
        // 根据_id字段计算所属的bucketNum
        int bucketNum = Math.abs(_id.hashCode() % maxBucketNum);
        // 如果当前表有唯一索引，则将其归入特殊的bucketNum=1中
        if (metadataOplog.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在，若不存在则添加
        if (!bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(currentDbTable, bucketNum);
            bucketSetMap.get(bucketNum).add(_id);
        }
        // 获取o2和o字段的值
        Document o2 = ((Document) document.get("o2"));
        Document o = (Document) document.get("o");
        o.remove("$v");
        // 判断是否是5.x和6.x更新语句修改了oplog的格式
        if (o.containsKey("diff") && !o.containsKey("_id")) {
            // 将diff字段解析为更新操作的$set和$unset
            Document updateAndInsertValue = new Document();
            Document deleteValue = new Document();
            final Document diff = o.get("diff", Document.class);
            for (Map.Entry<String, Object> entry : diff.entrySet()) {
                parseUpdateDiff(updateAndInsertValue, deleteValue, entry.getKey(), entry.getValue(), "");
            }
            Document up = new Document();
            if (!updateAndInsertValue.isEmpty()) {
                up.append("$set", updateAndInsertValue);
            }
            if (!deleteValue.isEmpty()) {
                up.append("$unset", deleteValue);
            }
            // 不可能出现 既无$set 也无$unset
            // 添加更新操作到bucketWriteModelListMap
            bucketWriteModelListMap.get(bucketNum).add(new UpdateOneModel<Document>(o2, up));
        } else if (o.get("$set") == null && o.get("$unset") == null) {
            // 有些oplog的o没有$set和$unset，此时表示有新增或替换的操作
            // 是否开启upsert
            ReplaceOptions option = new ReplaceOptions();
            option.upsert(true);
            // 添加替换操作到bucketWriteModelListMap
            bucketWriteModelListMap.get(bucketNum).add(new ReplaceOneModel<Document>(o2, o, option));
        } else {
            // 普通的更新操作，将o中的字段更新到o2中
            bucketWriteModelListMap.get(bucketNum).add(new UpdateOneModel<Document>(o2, o));
        }
    }

    /**
     * 解析diff数据
     *
     * @param updateAndInsertValue $set
     * @param deleteValue          $unset
     * @param key                  key
     * @param value                value
     * @param preKey               上一层级key
     * @desc 解析diff数据。逻辑复杂可以特殊讲解测试，执行update.json 观察oplog输出信息
     */
    private static void parseUpdateDiff(Document updateAndInsertValue, Document deleteValue, String key, Object value, String preKey) {
        // 如果key以"d"开头，表示删除操作
        if (key.startsWith("d")) {
            // 删除
            if (key.length() == 1) {
                if (preKey.length() == 0) {
                    deleteValue.putAll((Document) value);
                } else {
                    final Document document = (Document) value;
                    for (Map.Entry<String, Object> entry : document.entrySet()) {
                        deleteValue.put(preKey + "." + entry.getKey(), entry.getValue());
                    }
                }
            }
        } else if (key.startsWith("i")) {
            // 插入
            // 如果key以"i"开头，表示插入操作
            if (key.length() == 1) {
                if (preKey.length() == 0) {
                    updateAndInsertValue.putAll((Document) value);
                } else {
                    final Document document = (Document) value;
                    for (Map.Entry<String, Object> entry : document.entrySet()) {
                        updateAndInsertValue.put(preKey + "." + entry.getKey(), entry.getValue());
                    }
                }
            }
        } else if (key.startsWith("u")) {
            // 更新
            // 如果key以"u"开头，表示更新操作
            if (key.length() >= 2) {
                updateAndInsertValue.put(preKey + "." + key.replaceFirst("u", ""), value);
            }
            if (key.length() == 1) {
                if (preKey.length() == 0) {
                    updateAndInsertValue.putAll((Document) value);
                } else {
                    final Document document = (Document) value;
                    for (Map.Entry<String, Object> entry : document.entrySet()) {
                        updateAndInsertValue.put(preKey + "." + entry.getKey(), entry.getValue());
                    }
                }
            }

        } else if (key.startsWith("s")) {
            // 子项
            // 如果key以"s"开头，表示子项操作
            final Document document = (Document) value;
            if (document.containsKey("a") && "true".equals(document.get("a").toString())) {
                // 在数组里面的操作
                for (Map.Entry<String, Object> entry : document.entrySet()) {
                    String preKeyTemp = preKey.length() == 0 ? "" + key.replaceFirst("s", "") : preKey + "." + key.replaceFirst("s", "");
                    parseUpdateDiff(updateAndInsertValue, deleteValue, entry.getKey(), entry.getValue(), preKeyTemp);
                }
            } else {
                // 正常子类
                for (Map.Entry<String, Object> entry : document.entrySet()) {
                    String preKeyTemp = preKey.length() == 0 ? "" + key.replaceFirst("s", "") : preKey + "." + key.replaceFirst("s", "");
                    parseUpdateDiff(updateAndInsertValue, deleteValue, entry.getKey(), entry.getValue(), preKeyTemp);
                }
            }
        }
    }

    public static void main(String[] args) {
//        com.whaleal.ddt.parse.BucketOplogForGteMongoDB5 oplogNsBucketTaskV3 = new com.whaleal.ddt.parse.BucketOplogForGteMongoDB5(null, null);
//        oplogNsBucketTaskV3.parseUpdate(Document.com.whaleal.ddt.parse("{\"ts\": {\"$timestamp\": {\"t\": 1639979906, \"i\": 1}}, \"t\": 1, \"h\": 329795301790987907, \"v\": 2, \"op\": \"u\", \"ns\": \"photon.inventory\", \"ui\": {\"$binary\": {\"base64\": \"u0i0mE4nQn6xlBXy30wsYg==\", \"subType\": \"04\"}}, \"o2\": {\"_id\": {\"$oid\": \"61c01b602e2ab23f687ddb4a\"}}, \"wall\": {\"$date\": \"2021-12-20T05:58:26.613Z\"}, \"o\": {\"_id\": {\"$oid\": \"61c01b602e2ab23f687ddb4a\"}, \"item\": \"paper\", \"instock\": [{\"warehouse\": \"A\", \"qty\": 60.0}, {\"warehouse\": \"B\", \"qty\": 40.0}]}}\n"));
//
    }
}
