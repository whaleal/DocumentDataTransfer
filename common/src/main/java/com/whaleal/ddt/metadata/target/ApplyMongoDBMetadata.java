package com.whaleal.ddt.metadata.target;

import com.alibaba.fastjson2.JSON;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.whaleal.ddt.connection.MongoDBConnection;
import com.whaleal.ddt.metadata.util.ParserMongoStructureUtil;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@Data
@Log4j2
/**
 * ApplyMongoDBMetadata是一个负责在MongoDB上应用元数据操作的类。
 */
public class ApplyMongoDBMetadata {

    /**
     * 数据源名称。
     */
    private final String dsName;

    /**
     * MongoDB客户端。
     */
    private final MongoClient client;

    public ApplyMongoDBMetadata(String dsName, int createIndexThreadNum, long createIndexTimeOut) {
        this.dsName = dsName;
        this.client = MongoDBConnection.getMongoClient(dsName);
        this.createIndexThreadNum = createIndexThreadNum;
        this.createIndexTimeOut = createIndexTimeOut;
    }

    /**
     * 创建索引的线程数。 默认8个
     */
    private int createIndexThreadNum = 8;

    /**
     * 创建索引的超时时间。 默认超时1天
     */
    private long createIndexTimeOut = 3600 * 24L;

    /**
     * 创建MongoDB数据库中的集合。
     *
     * @param createCollectionOptionMap 包含集合名称及其创建选项的映射。
     */
    public void createCollection(Map<String, CreateCollectionOptions> createCollectionOptionMap) {
        for (Map.Entry<String, CreateCollectionOptions> entry : createCollectionOptionMap.entrySet()) {
            MongoNamespace mongoNamespace = new MongoNamespace(entry.getKey());
            CreateCollectionOptions options = entry.getValue();
            try {
                log.warn("{} try to pre-build the {} collection structure:{}", dsName, mongoNamespace.getFullName(), JSON.toJSONString(options));
                client.getDatabase(mongoNamespace.getDatabaseName()).createCollection(mongoNamespace.getCollectionName(), options);
            } catch (Exception e) {
                log.error("{} the attempt to pre-build the {} collection failed, the reason for the error:{}", dsName, mongoNamespace.getFullName(), e.getMessage());
            }
        }
    }

    /**
     * 在MongoDB数据库中创建视图。
     *
     * @param viewInfoMap 包含视图名称及其视图信息的映射。
     */
    public void createView(Map<String, Document> viewInfoMap) {
        for (Map.Entry<String, Document> entry : viewInfoMap.entrySet()) {
            MongoNamespace mongoNamespace = new MongoNamespace(entry.getKey());
            Document viewInfo = entry.getValue();
            try {
                Document options = viewInfo.get("options", Document.class);
                CreateViewOptions createViewOptions = new CreateViewOptions();
                if (options.containsKey("collation")) {
                    Document collation = options.get("collation", Document.class);
                    createViewOptions.collation(ParserMongoStructureUtil.parseCollation(collation));
                }
                String viewOn = options.get("viewOn").toString();
                List<Document> pipeline = options.getList("pipeline", Document.class);
                log.warn("{} try to pre-build the {} view structure:{}", dsName, mongoNamespace.getFullName(), options.toJson());
                client.getDatabase(mongoNamespace.getDatabaseName()).createView(mongoNamespace.getCollectionName(), viewOn, pipeline, createViewOptions);
            } catch (Exception e) {
                log.error("{} the attempt to pre-build the {} view failed, the reason for the error:{}", dsName, mongoNamespace.getFullName(), e.getMessage());
            }
        }
    }

    /**
     * 更新MongoDB的配置设置。
     *
     * @param configSetlist 要更新的配置设置列表。
     */
    public void updateConfigSetting(List<Document> configSetlist) {
        for (Document configSet : configSetlist) {
            try {
                log.warn("{} try to update the {} data :{}", dsName, "config.settings", configSet.toJson());
                client.getDatabase("config").getCollection("settings").updateOne(
                        new Document("_id", configSet.get("_id")), new Document("$set", configSet), new UpdateOptions().upsert(true));
            } catch (Exception e) {
                log.error("{} update Config.Setting failed, the reason for the error:{}", dsName, e.getMessage());
            }
        }
    }

    /**
     * 创建索引。
     *
     * @param indexQueue 索引队列。
     */
    public void createIndex(BlockingQueue<Document> indexQueue) {
        CountDownLatch countDownLatch = new CountDownLatch(createIndexThreadNum);
        Runnable runnable = () -> {
            while (!indexQueue.isEmpty()) {
                log.info("{} the remaining {} indexes have not been created", dsName, indexQueue.size());
                Document document = null;
                try {
                    document = indexQueue.poll();
                    if (document == null) {
                        break;
                    }
                    String indexName = document.get("name").toString();
                    if ("_id_".equals(indexName)) {
                        continue;
                    }
                    BasicDBObject index = new BasicDBObject();
                    Document key = (Document) document.get("key");
                    for (Map.Entry<String, Object> indexTemp : key.entrySet()) {
                        index.append(indexTemp.getKey(), indexTemp.getValue());
                    }
                    IndexOptions indexOptions = ParserMongoStructureUtil.parseIndexOptions(document);
                    MongoNamespace mongoNamespace = new MongoNamespace(document.get("ns").toString());
                    // 一定要设计true
                    indexOptions.background(true);
                    log.warn("{} ns:{},creating index:{}", dsName, document.get("ns").toString(), document.toJson());
                    client.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).createIndex(index, indexOptions);
                } catch (Exception e) {
                    log.error("{} failed to create index:{},msg:{}", dsName, document.toJson(), e.getMessage());
                }
            }
            // 信号量--
            countDownLatch.countDown();
        };

        for (int i = 0; i < createIndexThreadNum; i++) {
            // todo 可以改为线程池
            new Thread(runnable).start();
        }

        try {
            // 24小时还没有创建好 就强制退出
            countDownLatch.await(createIndexTimeOut, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 启用数据库的分片。
     *
     * @param shardingDBNameList 要启用分片的数据库名称列表。
     */
    public void enableShardingDataBase(List<String> shardingDBNameList) {
        for (String dbName : shardingDBNameList) {
            try {
                log.warn("{} 启用数据库{}分片", dsName, dbName);
                client.getDatabase("admin").runCommand(new Document("enableSharding", dbName));
            } catch (Exception e) {
                log.error("{} failed to enable sharding for the library,msg:{}", dsName, e.getMessage());
            }
        }
    }

    /**
     * 创建分片键。
     *
     * @param shardKeyList 分片键列表。
     */
    public void createShardKey(List<Document> shardKeyList) {
        for (Document shardKeyDoc : shardKeyList) {
            try {
                client.getDatabase("admin").
                        runCommand(new Document("enableSharding",
                                new MongoNamespace(shardKeyDoc.get("shardCollection").toString()).getDatabaseName()));
                log.warn("{} 创建分片键:{}", dsName, shardKeyDoc.toJson());
                client.getDatabase("admin").runCommand(shardKeyDoc);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} database table creation shard key,msg:{}", dsName, e.getMessage());
            }
        }
    }

    /**
     * 拆分分片表。
     *
     * @param splitList 拆分列表。
     */
    public void splitShardTable(List<Document> splitList) {
        for (Document splitDoc : splitList) {
            try {
                log.warn("{} 分片块切分:{}", dsName, splitDoc.toJson());
                client.getDatabase("admin").runCommand(splitDoc);
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * 删除表。
     *
     * @param ns 要删除的表的命名空间。
     */
    public void dropTable(String ns) {
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        try {
            log.warn("{} try to drop the table {}", dsName, ns);
            client.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).drop();
        } catch (Exception ignored) {
        }

    }
}
