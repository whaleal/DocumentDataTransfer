package com.whaleal.ddt.write;


import com.whaleal.ddt.cache.MetadataOplog;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.whaleal.ddt.cache.BatchDataEntity;
import com.whaleal.ddt.connection.MongoDBConnection;
import com.whaleal.ddt.metadata.util.WriteModelUtil;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @desc: 写入数据
 * @author: lhp
 * @time: 2021/7/30 11:56 上午
 */
@Log4j2
public class WriteData extends CommonTask {

    /**
     * oplog元数据库类
     */
    private final MetadataOplog metadataOplog;
    /**
     * mongoClient
     */
    private final MongoClient mongoClient;


    private final int bucketSize;


    public WriteData(String workName, String dsName, int bucketSize) {
        super(workName, dsName);
        this.metadataOplog = MetadataOplog.getOplogMetadata(workName);
        this.dsName = dsName;
        this.mongoClient = MongoDBConnection.getMongoClient(dsName);
        this.workName = workName;
        this.bucketSize = bucketSize;
    }

    @Override
    public void execute() {
        log.warn("{} the oplog com.whaleal.ddt.write data thread starts running", workName);
        int idlingTime = 0;
        while (true) {
            try {
                {
                    // 检查任务状态
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_STOP) {
                        break;
                    }
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                        TimeUnit.SECONDS.sleep(5);
                    }
                }
                if (idlingTime++ > 10) {
                    // 10次都没有获取到oplog信息,则进行睡眠
                    TimeUnit.SECONDS.sleep(1);
                    // 10次都没有获得锁 更有可能继续无法获得'锁'
                    idlingTime = 9;
                }
                for (Map.Entry<Integer, BlockingQueue<BatchDataEntity>> next : metadataOplog.getQueueOfBucketMap().entrySet()) {
                    // 桶号
                    Integer bucketNum = next.getKey();
                    // 为空就不用进来处理了  也不用进行加锁信息
                    if (next.getValue().isEmpty()) {
                        idlingTime++;
                        continue;
                    }
                    AtomicBoolean atomicBoolean = metadataOplog.getStateOfBucketMap().get(bucketNum);
                    boolean pre = atomicBoolean.get();
                    if (!pre && atomicBoolean.compareAndSet(false, true)) {
                        try {
                            // 解析后的WriteModel队列
                            Queue<BatchDataEntity> documentQueue = next.getValue();
                            idlingTime = 0;
                            // 写入该表的数据
                            write(documentQueue, bucketNum);
                        } catch (Exception e) {
                            log.error("bucketNum:{},an error occurred while writing the oplog,msg:{}", bucketNum, e.getMessage());
                        } finally {
                            atomicBoolean.set(false);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} an error occurred while writing the oplog,msg:{}", workName, e.getMessage());
            }
        }

    }

    /**
     * com.whaleal.ddt.write
     *
     * @param documentQueue 队列数据
     * @param bucketNum     桶号
     * @desc 执行写入
     */
    public void write(Queue<BatchDataEntity> documentQueue, int bucketNum) {
        int parseSize = 0;
        while (true) {
            BatchDataEntity batchDataEntity = documentQueue.poll();
            // batchDataEntity为null或已经写入了20批数据，则退出
            if (batchDataEntity == null) {
                break;
            }
            bulkExecute(batchDataEntity);
            // 有数据就一直写入
            // 一直有数据 就一直追加 此时大表中大幅度占有的时候 会阻塞其他线程的处理
            if (parseSize++ > bucketSize * 1024 * 10) {
                break;
            }
        }
    }

    /**
     * bulkExecute 批量写数据
     *
     * @param batchDataEntity 批数据
     * @desc 批量写数据
     */
    public void bulkExecute(BatchDataEntity batchDataEntity) {
        String dbTableName = batchDataEntity.getNs();
        List<WriteModel<Document>> list = batchDataEntity.getDataList();
        try {
            if (list.isEmpty()) {
                return;
            }
            String dbName = dbTableName.split("\\.", 2)[0];
            String tableName = dbTableName.split("\\.", 2)[1];
            final BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
            if (metadataOplog.getUniqueIndexCollection().containsKey(dbName + "." + tableName)) {
                bulkWriteOptions.ordered(true);
            }
            // todo 写入方式 要适配不同版本mongodb 最好可以自定义
            BulkWriteResult bulkWriteResult = this.mongoClient.getDatabase(dbName).getCollection(tableName).
                    withWriteConcern(WriteConcern.W1.withJournal(true)).bulkWrite(list, bulkWriteOptions);
            bulkWriteInfo(bulkWriteResult);
        } catch (Exception e) {
            // 出现异常 就一条一条数据写入
            singleExecute(batchDataEntity);
        }
    }

    /**
     * singleExecute 批量写数据
     *
     * @param batchDataEntity 批数据
     * @desc 批量写数据
     */
    public void singleExecute(BatchDataEntity batchDataEntity) {
        String ns = batchDataEntity.getNs();
        List<WriteModel<Document>> list = batchDataEntity.getDataList();
        // todo 注意
        MongoNamespace mongoNamespace = new MongoNamespace(ns + "_temp");
        for (WriteModel<Document> writeModel : list) {
            try {
                List<WriteModel<Document>> writeModelList = new ArrayList<>();
                writeModelList.add(writeModel);
                BulkWriteResult bulkWriteResult = this.mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).
                        withWriteConcern(WriteConcern.W1.withJournal(true)).bulkWrite(writeModelList, new BulkWriteOptions().ordered(true));
                bulkWriteInfo(bulkWriteResult);
            } catch (MongoBulkWriteException e) {
                for (BulkWriteError error : e.getWriteErrors()) {
                    log.error("ns:{},failed to com.whaleal.ddt.write data:{}", ns, error.getMessage());
                }
                log.error("ns:{},com.whaleal.ddt.write failed:{}", ns, WriteModelUtil.writeModelToString(writeModel));
            } catch (Exception e) {
                log.error("ns:{},failed to com.whaleal.ddt.write data:{}", ns, e.getMessage());
                log.error("ns:{},com.whaleal.ddt.write failed:{}", ns, WriteModelUtil.writeModelToString(writeModel));
            }
        }
    }

    /**
     * bulkWriteInfo 计算批数据写入情况
     *
     * @param bulkWriteResult 批数据写入情况
     * @desc 计算批数据写入情况
     */
    private void bulkWriteInfo(BulkWriteResult bulkWriteResult) {
        int insertedCount = bulkWriteResult.getInsertedCount();
        int deletedCount = bulkWriteResult.getDeletedCount();
        int modifiedCount = bulkWriteResult.getModifiedCount();
        metadataOplog.updateBulkWriteInfo("insert", insertedCount);
        metadataOplog.updateBulkWriteInfo("update", modifiedCount);
        metadataOplog.updateBulkWriteInfo("delete", deletedCount);
    }
}
