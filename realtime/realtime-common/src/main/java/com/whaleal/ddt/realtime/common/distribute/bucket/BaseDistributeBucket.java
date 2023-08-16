package com.whaleal.ddt.realtime.common.distribute.bucket;


import com.mongodb.client.MongoClient;
import com.mongodb.client.model.WriteModel;
import com.whaleal.ddt.cache.BatchDataEntity;
import com.whaleal.ddt.realtime.common.cache.MetaData;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.sync.connection.MongoDBConnectionSync;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author liheping
 */
@Log4j2
public abstract class BaseDistributeBucket<T> extends CommonTask implements ParseEventInterface<T> {

    /**
     * 等待ddl时间
     */
    protected final Integer ddlWait;
    /**
     * oplog元数据库
     */
    protected final MetaData<T> metadata;
    /**
     * k桶号 默认[0-16)
     * v为Set<id>
     */
    protected final Map<Integer, Set<String>> bucketSetMap = new HashMap<>();
    /**
     * k桶号 默认[0-16)
     * v为解析好的WriteModel数据集合
     */
    protected final Map<Integer, List<WriteModel<Document>>> bucketWriteModelListMap = new HashMap<>();
    /**
     * mongoClient
     */
    protected MongoClient mongoClient;
    /**
     * 当前解析的表maxTableBatchNumOfBucket
     */
    protected String currentDbTable;
    /**
     * 桶个数
     */
    protected final int maxBucketNum;
    /**
     * 要同步的DDL列表
     */
    protected final Set<String> ddlSet;

    protected BaseDistributeBucket(String workName, String dsName, int maxBucketNum, Set<String> ddlSet, int ddlWait) {
        super(workName, dsName);
        this.metadata = MetaData.getMetaData(workName);
        this.maxBucketNum = maxBucketNum;
        this.ddlSet = ddlSet;
        this.workName = workName;
        this.ddlWait = ddlWait;
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);
    }

    public void exe() {
        int idlingTime = 0;
        while (true) {
            try {
                // 判断任务状态
                if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_STOP) {
                    log.info("{} the {} thread exits from parsing the bucket", workName, Thread.currentThread().getName());
                    break;
                }
                if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                    // 发生了限速就开始限制读取
                    while (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                        TimeUnit.SECONDS.sleep(5);
                    }
                }
                /*
                 * 保存每个表的document
                 * k为表名，v为ns解析后的Document
                 */
                if (idlingTime++ > 10) {
                    // 10次都没有获取到oplog信息,则进行睡眠
                    TimeUnit.SECONDS.sleep(1);
                    // 10次都没有获得锁 更有可能继续无法获得'锁'
                    idlingTime = 9;
                }

                for (Map.Entry<String, BlockingQueue<T>> next : metadata.getQueueOfNsMap().entrySet()) {
                    // 表名
                    String dbTableName = next.getKey();
                    // 为空就不用进来处理了  也不用进行加锁信息
                    if (next.getValue().isEmpty()) {
                        idlingTime++;
                        continue;
                    }
                    // 当前线程要操作的表名进行赋值
                    this.currentDbTable = dbTableName;
                    // 加'锁',每个数据表最多同时有一个线程解析
                    AtomicBoolean atomicBoolean = metadata.getStateOfNsMap().get(dbTableName);
                    boolean pre = atomicBoolean.get();
                    // cas操作
                    if (!pre && atomicBoolean.compareAndSet(false, true)) {
                        try {
                            // 队列数据
                            Queue<T> documentQueue = next.getValue();
                            // 该表内有数据,idlingTime轮转次数设为0
                            idlingTime = 0;
                            // 对该表的bucketSetMap,bucketWriteModelListMap进行重新赋值
                            init();
                            // 解析队列中的数据
                            parse(documentQueue);
                            // 解析后把数据放入下一层级
                            putDataToCache();
                        } catch (Exception e) {
                            log.error("{} {} an error occurred in the bucketing thread of changeStream, the error message:{}", workName, dbTableName, e.getMessage());
                        } finally {
                            // 释放'锁'
                            atomicBoolean.set(false);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("{} an error occurred in the bucketing thread of changeStream, the error message:{}", workName, e.getMessage());
            }
        }
    }

    /**
     * init
     *
     * @desc 初始化该表的信息
     */
    public void init() {
        for (int i = 0; i < maxBucketNum; i++) {
            bucketSetMap.put(i, new HashSet<>());
            bucketWriteModelListMap.put(i, new ArrayList());
        }
    }

    public abstract void parse(Queue<T> eventQueue);

    @Override
    public void updateUniqueIndexCount(String ns) {
        String[] nsSplit = ns.split("\\.", 2);
        int count = 0;
        try {
            for (Document index : mongoClient.getDatabase(nsSplit[0]).getCollection(nsSplit[1]).listIndexes()) {
                // 可以考虑一下unique的值为1
                if (index.containsKey("unique") && "true".equals(index.get("unique").toString())) {
                    count++;
                }
            }
            metadata.getUniqueIndexCollection().put(ns, count);
        } catch (Exception e) {
            metadata.getUniqueIndexCollection().put(ns, count + 1);
            log.error("{} failed to get {} collection index, the error message is:{}", workName, ns, e.getMessage());
        }
        // 如果表不存在唯一索引的话 可以进行删除此key 防止堆积太多ns
        if (metadata.getUniqueIndexCollection().getOrDefault(ns, 0) == 0) {
            metadata.getUniqueIndexCollection().remove(ns);
        }
    }

    /**
     * putDataToCache
     *
     * @param ns        库表民
     * @param bucketNum 桶号
     * @desc 添加数据到下一层级
     */
    public void putDataToCache(String ns, int bucketNum) {
        try {
            // 16个公共区
            int nsBucketNum = Math.abs((ns + bucketNum).hashCode() % maxBucketNum);
            if (metadata.getUniqueIndexCollection().containsKey(ns)) {
                nsBucketNum = Math.abs(ns.hashCode() % maxBucketNum);
            }
            BatchDataEntity batchDataEntity = new BatchDataEntity();
            batchDataEntity.setNs(ns);
            batchDataEntity.setDataList(bucketWriteModelListMap.get(bucketNum));
            metadata.getQueueOfBucketMap().get(nsBucketNum).put(batchDataEntity);
            // 修改map中的信息
            bucketSetMap.put(bucketNum, new HashSet<>());
            bucketWriteModelListMap.put(bucketNum, new ArrayList());
        } catch (Exception e) {
            log.error("{} an exception occurred when adding data to the event thread, the error message:{}", workName, e.getMessage());
        }
    }

    /**
     * putDataToCache 添加数据到下一层级
     *
     * @desc 添加所有数据到下一层级
     */
    public void putDataToCache() {
        Set<Integer> keySet = bucketWriteModelListMap.keySet();
        for (Integer bucketNum : keySet) {
            putDataToCache(currentDbTable, bucketNum);
        }
    }

    public abstract void parseDDL(T event);
}
