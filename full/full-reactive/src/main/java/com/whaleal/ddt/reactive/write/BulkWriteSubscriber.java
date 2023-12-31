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
package com.whaleal.ddt.reactive.write;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.WriteModel;
import com.whaleal.ddt.cache.BatchDataEntity;
import com.whaleal.ddt.common.cache.FullMetaData;
import com.whaleal.ddt.util.WriteModelUtil;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonDocument;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;

@NoArgsConstructor
@Log4j2
public class BulkWriteSubscriber implements Subscriber<BulkWriteResult> {

    private Subscription subscription;

    private FullMetaData fullMetaData;

    private List<WriteModel<BsonDocument>> writeModelList;

    private String ns;

    private String workName;

    public BulkWriteSubscriber(FullMetaData fullMetaData, List<WriteModel<BsonDocument>> writeModelList, String ns, String workName) {
        this.fullMetaData = fullMetaData;
        this.writeModelList = writeModelList;
        this.ns = ns;
        this.workName = workName;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(BulkWriteResult bulkWriteResult) {
        System.out.println(Thread.currentThread().getName());
        int insertedCount = bulkWriteResult.getInsertedCount();
        fullMetaData.getWriteDocCount().add(insertedCount);
    }

    @Override
    public void onError(Throwable throwable) {

        if (this.writeModelList.isEmpty()) {
            return;
        }
        if (writeModelList.size() == 1) {
            if (throwable instanceof MongoBulkWriteException) {
                MongoBulkWriteException e = (MongoBulkWriteException) throwable;
                // 如果写入出现异常，打印错误信息
                for (BulkWriteError error : e.getWriteErrors()) {
                    log.error("ns:{},data write failure:{}", ns, error.getMessage());
                }
            } else {
                // 如果写入出现其他异常，打印错误信息
                log.error("ns:{},data write failure:{}", ns, throwable.getMessage());
            }
            for (WriteModel<BsonDocument> writeModel : writeModelList) {
                log.error("ns:{},data write failure:{}", ns, WriteModelUtil.writeModelToString(writeModel));
            }
        } else {
            for (WriteModel<BsonDocument> writeModel : writeModelList) {
                // 当发生异常 把数据重新放回队列 重新写入
                BatchDataEntity<WriteModel<BsonDocument>> batchDataEntity = new BatchDataEntity();
                ArrayList<WriteModel<BsonDocument>> arrayList = new ArrayList<>();
                arrayList.add(writeModel);
                batchDataEntity.setDataList(arrayList);
                batchDataEntity.setNs(ns);
                batchDataEntity.setSourceDsName("");
                batchDataEntity.setBatchNo(System.currentTimeMillis());
                // 推送数据到缓存区中
                this.fullMetaData.putData(batchDataEntity);
            }
        }
    }

    @Override
    public void onComplete() {
        // 可以不打印信息
        writeModelList = null;
    }


}
