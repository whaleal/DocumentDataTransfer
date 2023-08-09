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
package com.whaleal.ddt.sync.parse.ns;

import com.whaleal.ddt.sync.cache.MetadataOplog;
import com.mongodb.client.MongoClient;
import com.whaleal.ddt.connection.MongoDBConnection;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author: lhp
 * @time: 2021/7/21 2:38 下午
 * @desc: 解析document的ns
 */
@Log4j2
public class ParseOplogNs extends CommonTask {
    /**
     * oplog元数据库类
     */
    private final MetadataOplog metadataOplog;
    /**
     * 表过滤策略 正则表达式处理,着重处理ddl操作表
     */
    private final String dbTableWhite;
    /**
     * mongoClient
     */
    private final MongoClient mongoClient;
    /**
     * 每个ns队列最大缓存个数
     */
    private final int maxQueueSizeOfNs;
    /**
     * 是否执行删库操作
     */
    private boolean isDropDataBase = false;

    public ParseOplogNs(String workName, String dbTableWhite, String dsName, int maxQueueSizeOfNs, boolean isDropDataBase) {
        super(workName, dsName);
        this.dbTableWhite = dbTableWhite;
        this.workName = workName;
        this.metadataOplog = MetadataOplog.getOplogMetadata(workName);
        this.mongoClient = MongoDBConnection.getMongoClient(dsName);
        this.maxQueueSizeOfNs = maxQueueSizeOfNs;
        this.isDropDataBase = isDropDataBase;
    }

    @Override
    public void execute() {
        log.info("{} oplog parsing ns thread starts running", workName);
        // 当前解析oplog日志的个数
        exe();
    }

    private void exe() {
        int count = 0;
        // 上次清除ns的时间
        long lastCleanNsTime = System.currentTimeMillis();
        while (true) {
            // 要加上异常处理 以防出现解析ns的线程异常退出
            Document document = null;
            try {
                // 从原始的oplogList进行解析，获取对应的ns
                document = metadataOplog.getQueueOfOplog().poll();
                if (document != null) {
                    // 解析ns
                    parseNs(document);
                } else {
                    // 要是oplog太慢,count增加,lastCleanNsTime减少,此时也及时进行清除ns。
                    count += 1000;
                    lastCleanNsTime -= 1000;
                    // 代表oplog队列为空 暂时休眠
                    TimeUnit.SECONDS.sleep(1);
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_STOP) {
                        break;
                    }
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                        // 发生了限速就开始限制读取
                        while (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                            TimeUnit.SECONDS.sleep(5);
                        }
                    }
                }
                // 每100w条 && 10分钟 清除一下空闲ns表信息
                if (count++ > 1000000) {
                    count = 0;
                    long currentTimeMillis = System.currentTimeMillis();
                    // 10分钟
                    if ((currentTimeMillis - lastCleanNsTime) > 1000 * 60 * 10) {
                        lastCleanNsTime = currentTimeMillis;
                        log.warn("{} start removing redundant ns buckets", workName);
                        for (Map.Entry<String, BlockingQueue<Document>> queueEntry : metadataOplog.getQueueOfNsMap().entrySet()) {
                            try {
                                BlockingQueue<Document> value = queueEntry.getValue();
                                String key = queueEntry.getKey();
                                AtomicBoolean atomicBoolean = metadataOplog.getStateOfNsMap().get(key);
                                boolean pre = atomicBoolean.get();
                                // cas操作
                                if (value.isEmpty() && !pre && atomicBoolean.compareAndSet(false, true)) {
                                    metadataOplog.getQueueOfNsMap().remove(key);
                                    metadataOplog.getStateOfNsMap().remove(key);
                                    atomicBoolean.set(false);
                                }
                            } catch (Exception e) {
                                log.error("{} error in clearing free ns queue of oplog,msg:{}", workName, e.getMessage());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (document != null) {
                    log.error("{} currently parsing the oplog log:{}", workName, document.toJson());
                }
                log.error("{} an error occurred in the split table thread of the oplog,msg:{}", workName, e.getMessage());
            }
        }
    }

    /**
     * parseNs
     *
     * @param document oplog信息
     * @desc 解析document的ns
     */
    public void parseNs(Document document) throws InterruptedException {
        String fullDbTableName = document.get("ns").toString();
        String op = document.get("op").toString();
        boolean isDDL = false;
        // DDL
        if ("c".equals(op)) {
            fullDbTableName = parseDDL(document);
            // 判读ddl的数据操作的ns是否符合表名过滤
            if (fullDbTableName.length() == 0 || !fullDbTableName.matches(dbTableWhite)) {
                return;
            }
            isDDL = true;
        }
        String tableName = fullDbTableName.split("\\.", 2)[1];
        // system.buckets.
        // 5.0以后分桶表 可以存储数据 可以参考system.txt说明
        if (tableName.startsWith("system.") && (!tableName.startsWith("system.buckets."))) {
            return;
        }
        // 多重DDL 保证DDL顺序性问题
        if (isDDL) {
            metadataOplog.waitCacheExe();
        }
        if (!metadataOplog.getQueueOfNsMap().containsKey(fullDbTableName)) {
            metadataOplog.getQueueOfNsMap().put(fullDbTableName, new LinkedBlockingQueue<>(maxQueueSizeOfNs));
            metadataOplog.getStateOfNsMap().put(fullDbTableName, new AtomicBoolean());
            // 更新此表的唯一索引情况
            Document updateIndexInfo = new Document();
            updateIndexInfo.put("op", "updateIndexInfo");
            metadataOplog.getQueueOfNsMap().get(fullDbTableName).put(updateIndexInfo);
        }
        metadataOplog.getQueueOfNsMap().get(fullDbTableName).put(document);
        if (isDDL) {
            metadataOplog.waitCacheExe();
        }
    }

    /**
     * parseDDL
     *
     * @param document oplog中DDL相关日志
     * @desc 解析DDL document的ns
     */
    public String parseDDL(Document document) {
        Document o = (Document) document.get("o");
        String fullDbTableName = "";
        if (o.get("create") != null) {
            fullDbTableName = parseCreateTable(document);
        } else if (o.get("drop") != null) {
            fullDbTableName = parseDropTable(document);
        } else if (o.get("createIndexes") != null) {
            fullDbTableName = parseCreateIndex(document);
        } else if (o.get("commitIndexBuild") != null) {
            fullDbTableName = parseCommitIndexBuild(document);
        } else if (o.get("dropIndexes") != null) {
            fullDbTableName = parseDropIndex(document);
        } else if (o.get("renameCollection") != null) {
            fullDbTableName = parseRenameTable(document);
        } else if (o.get("convertToCapped") != null) {
            // Q: 可以加上此功能
            // A: convertToCapped=drop+create
        } else if (o.get("dropDatabase") != null) {
            parseDropDataBase(document);
        } else if (o.get("collMod") != null) {
            return parseCollMod(document);
        }
        return fullDbTableName;
    }

    /**
     * parseDropTable
     *
     * @param document oplog
     * @desc 解析DropTableDocument的ns
     */
    public String parseDropTable(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("drop").toString();
        return dbName + "." + tableName;
    }

    /**
     * parseDropDataBase
     *
     * @param document oplog
     * @desc 解析删库
     */
    public void parseDropDataBase(Document document) {
        // 此方法 不会用到 删除的语句 会变成删除n个删除表语句
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        if (!(dbName + ".").matches(dbTableWhite)) {
            return;
        }
        log.warn("{} drop database operation:{}", workName, document.toJson());
        if (isDropDataBase) {
            // 等待缓存中数据写完
            metadataOplog.waitCacheExe();
            // 后续数据都写入后 进行删库操作
            mongoClient.getDatabase(dbName).drop();
            log.warn("{} the drop database operation is complete:{}", workName, dbName);
        }
    }

    /**
     * parseCreateTable
     *
     * @param document oplog
     * @desc 解析createTableDocument的ns
     */
    public String parseCreateTable(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("create").toString();
        return dbName + "." + tableName;
    }

    /**
     * parseRenameTable
     *
     * @param document oplog
     * @desc 解析RenameTableDocument的ns
     */
    public String parseRenameTable(Document document) {
        Document o = (Document) document.get("o");
        return o.get("renameCollection").toString();
    }

    /**
     * parseCreateIndex
     *
     * @param document oplog
     * @desc 解析CreateIndexDocument的ns
     */
    public String parseCreateIndex(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("createIndexes").toString();
        return dbName + "." + tableName;
    }

    /**
     * parseCommitIndexBuild
     *
     * @param document oplog
     * @desc 解析commitIndexBuild的ns
     */
    public String parseCommitIndexBuild(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("commitIndexBuild").toString();
        return dbName + "." + tableName;
    }

    /**
     * parseDropIndex
     *
     * @param document oplog
     * @desc 解析DropIndexDocument的ns
     */
    public String parseDropIndex(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("dropIndexes").toString();
        return dbName + "." + tableName;
    }

    /**
     * parseCollMod
     *
     * @param document oplog
     * @desc 解析parseCollMod的ns
     */
    public String parseCollMod(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("collMod").toString();
        return dbName + "." + tableName;
    }

}
