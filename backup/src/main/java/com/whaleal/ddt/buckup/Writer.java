package com.whaleal.ddt.buckup;


import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.whaleal.ddt.cache.BatchDataEntity;
import com.whaleal.ddt.util.WriteModelUtil;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@Log4j2
public class Writer implements Runnable {

    private static LongAdder totalWriteNum = new LongAdder();

    public static long getTotalWriteNum() {
        return totalWriteNum.sum();
    }

    Cache<Document> documentCache;

    private String targetNS;

    private MongoClient mongoClient;

    public Writer(Cache<Document> documentCache, String targetNS, MongoClient mongoClient) {
        this.documentCache = documentCache;
        this.targetNS = targetNS;
        this.mongoClient = mongoClient;
    }

    @Override
    public void run() {
        while (true) {
            write(documentCache.getQueueOfEvent());
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * write
     *
     * @param documentQueue 队列数据
     * @desc 执行写入
     */
    public void write(Queue<Document> documentQueue) {

        int parseSize = 0;
        while (true) {
            BatchDataEntity<InsertOneModel<Document>> batchDataEntity = new BatchDataEntity();
            ArrayList<InsertOneModel<Document>> documentList = new ArrayList<>();
            for (int i = 128; i > 0; i--) {
                Document poll = documentQueue.poll();
                if (poll == null) {
                    continue;
                }
                documentList.add(new InsertOneModel<>(poll));
            }
            batchDataEntity.setDataList(documentList);
            batchDataEntity.setNs(targetNS);
            bulkExecute(batchDataEntity);
            if (parseSize++ > 102400) {
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
            BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);

            // q: 写入方式
            // a: 用户自己在url上配置
            BulkWriteResult bulkWriteResult = this.mongoClient.getDatabase(dbName).getCollection(tableName).
                    bulkWrite(list, bulkWriteOptions);
            totalWriteNum.add(bulkWriteResult.getInsertedCount());
        } catch (Exception e) {
            // 出现异常 就一条一条数据写入
            // todo 可以更加优化处理
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
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        for (WriteModel<Document> writeModel : list) {
            try {
                List<WriteModel<Document>> writeModelList = new ArrayList<>();
                writeModelList.add(writeModel);
                // q: 写入方式
                // a: 用户自己在url上配置
                BulkWriteResult bulkWriteResult = this.mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).
                        bulkWrite(writeModelList, new BulkWriteOptions().ordered(true));
                totalWriteNum.add(bulkWriteResult.getInsertedCount());
            } catch (MongoBulkWriteException e) {
                for (BulkWriteError error : e.getWriteErrors()) {
                    log.error("ns:{},failed to write data:{}", ns, error.getMessage());
                }
                log.error("ns:{},write failed:{}", ns, WriteModelUtil.writeModelToString(writeModel));
            } catch (Exception e) {
                log.error("ns:{},failed to write data:{}", ns, e.getMessage());
                log.error("ns:{},write failed:{}", ns, WriteModelUtil.writeModelToString(writeModel));
            }
        }
    }


}
