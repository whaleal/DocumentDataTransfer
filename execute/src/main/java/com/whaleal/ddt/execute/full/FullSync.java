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
package com.whaleal.ddt.execute.full;


import com.whaleal.ddt.common.Datasource;
import com.whaleal.ddt.common.generate.Range;
import com.whaleal.ddt.common.generate.SubmitSourceTask;
import com.whaleal.ddt.conection.sync.MongoDBConnectionSync;
import com.whaleal.ddt.execute.full.common.BaseFullWork;
import com.whaleal.ddt.sync.task.read.FullSyncReadTask;
import com.whaleal.ddt.sync.task.write.FullSyncWriteTask;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author liheping
 */
@Log4j2
public class FullSync extends BaseFullWork {

    /**
     * FullSync类的构造函数。
     *
     * @param workName 工作名称。
     */
    public FullSync(String workName) {
        super(workName);
    }

    @Override
    public void submitTargetTask(int writeThreadNum) {
        for (int i = 0; i < writeThreadNum; i++) {
            createTask(writeThreadPoolName, new FullSyncWriteTask(workName, targetDsName));
        }
    }

    @Override
    public void generateSource(int readThreadNum, BlockingQueue<Range> taskQueue, AtomicInteger isGenerateSourceTaskInfoOverNum, int batchSize) {
        SubmitSourceTask submitSourceTask = new SubmitSourceTask(workName, sourceDsName,
                readThreadNum, taskQueue, isGenerateSourceTaskInfoOverNum, batchSize, readThreadPoolName, FullSyncReadTask.class);
        createTask(commonThreadPoolName, submitSourceTask);
    }

    @Override
    public void initConnection(String dsName, String url) {
        MongoDBConnectionSync.createMonoDBClient(dsName, new Datasource(url));
    }
}
