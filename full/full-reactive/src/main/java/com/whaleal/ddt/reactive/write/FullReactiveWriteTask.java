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


import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.WriteModel;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.whaleal.ddt.common.cache.FullMetaData;
import com.whaleal.ddt.common.write.BaseFullWriteTask;
import com.whaleal.ddt.conection.reactive.MongoDBConnectionReactive;
import com.whaleal.ddt.task.CommonTask;
import com.whaleal.ddt.thread.pool.ThreadPoolManager;
import io.reactivex.rxjava3.internal.schedulers.ExecutorScheduler;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonDocument;
import org.bson.Document;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 写入数据任务类
 * 继承自通用任务类{@link CommonTask}
 * 用于将缓存中的批量数据写入到MongoDB数据库中
 *
 * @author liheping
 */
@Log4j2
public class FullReactiveWriteTask extends BaseFullWriteTask {

    /**
     * mongoClient 客户端连接对象，用于与MongoDB进行数据交互
     */
    private final com.mongodb.reactivestreams.client.MongoClient mongoClient;

    /**
     * 构造函数
     *
     * @param workName 工作名称，用于任务标识
     * @param dsName   数据源名称，用于获取对应的MongoDB连接
     */
    public FullReactiveWriteTask(String workName, String dsName) {
        // 调用父类的构造函数，初始化工作名称和数据源名称
        super(workName, dsName);
        mongoClient = MongoDBConnectionReactive.getMongoClient(dsName);
    }


    @Override
    public int bulkExecute(String ns, List<WriteModel<BsonDocument>> writeModelList) {
        if (writeModelList.isEmpty()) {
            return 0;
        }

        // 没加上队列限制
        // 此处放入另一个另外一个线程池
        // 注意区分 多线程的异步情况
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        MongoCollection<BsonDocument> collection = mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName(),BsonDocument.class);

        Publisher<BulkWriteResult> publisher = collection.bulkWrite(writeModelList, BULK_WRITE_OPTIONS);

        BulkWriteSubscriber subscriber = new BulkWriteSubscriber(FullMetaData.getFullMetaData(workName), writeModelList, ns, workName);
        publisher.subscribe(subscriber);

        Flux.from(publisher).
                subscribeOn(Schedulers.fromExecutor(ThreadPoolManager.getPool(workName + "_writeOfBulkThreadPoolName").getExecutorService()));

        return 0;
    }

}
