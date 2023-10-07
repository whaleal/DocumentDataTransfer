package test;

import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.whaleal.ddt.cache.BatchDataEntity;
import com.whaleal.ddt.common.Datasource;
import com.whaleal.ddt.common.cache.FullMetaData;
import com.whaleal.ddt.conection.reactive.MongoDBConnectionReactive;
import com.whaleal.ddt.conection.sync.MongoDBConnectionSync;
import com.whaleal.ddt.thread.pool.ThreadPoolManager;
import org.bson.BsonDocument;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

public class ReactiveMongodb {


    /**
     * 初始化MongoDB连接的方法，通过给定的数据源名称(dsName)和URL(url)创建MongoDB连接。
     *
     * @param dsName 数据源名称
     * @param url    数据源连接URL
     */
    private static void initConnection(String dsName, String url) {
        MongoDBConnectionSync.createMonoDBClient(dsName, new Datasource(url));
        MongoDBConnectionReactive.createMonoDBClient(dsName, new Datasource(url));
    }

    /**
     * 初始化线程池的方法，通过给定的线程池名称(threadPoolName)和核心线程数(corePoolSize)创建一个线程池。
     *
     * @param threadPoolName 线程池名称
     * @param corePoolSize   线程池的核心线程数
     */
    private static void intiThreadPool(String threadPoolName, int corePoolSize) {
        ThreadPoolManager manager = new ThreadPoolManager(threadPoolName, corePoolSize, corePoolSize, Integer.MAX_VALUE);
    }


    public static LongAdder longAdder = new LongAdder();

    public static void main(String[] args) {
        String workName = "test";
        String sourceName = "source_test";
        String targetName = "target_test";

        intiThreadPool(workName + "_source", 10);
        intiThreadPool(workName + "_target", 10);


        initConnection(sourceName, "mongodb://192.168.12.200:24578");
        initConnection(targetName, "mongodb://192.168.12.100:24578");

        FullMetaData fullMetaData = new FullMetaData(workName, 20, 20);


        for (String databaseName : MongoDBConnectionSync.getMongoClient(sourceName).listDatabaseNames()) {
            if (databaseName.equals("local") || databaseName.equals("admin") || databaseName.equals("config")) {
                continue;
            }

            for (String collectionName : MongoDBConnectionSync.getMongoClient(sourceName).getDatabase(databaseName).listCollectionNames()) {
//                if (databaseName.equals("source"))
                {
                    MongoDBConnectionSync.getMongoClient(targetName).getDatabase(databaseName).getCollection(collectionName, BsonDocument.class).drop();
                    readAndPublishFromCollection(
                            MongoDBConnectionReactive.getMongoClient(sourceName).getDatabase(databaseName).getCollection(collectionName, BsonDocument.class)

                    );
                }
            }
        }

        // Add more collections as needed
        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
//        consumeFromBuffer();
        long old = longAdder.sum();
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long newValue = longAdder.sum();
            System.out.println(newValue - old);
            old = newValue;
        }
    }

    private static Flux<BsonDocument> source(FindPublisher<BsonDocument> documentFindPublisher) {
        return Flux.defer(() -> {
            // 在这里指定线程池名称，并在该线程池中执行订阅的代码块
            return Flux.merge(documentFindPublisher)
                    .subscribeOn(Schedulers.fromExecutor(ThreadPoolManager.getPool("test_source").getExecutorService()));
        });
    }

    private static void readAndPublishFromCollection(MongoCollection<BsonDocument> collection) {

        FindPublisher<BsonDocument> documentFindPublisher = collection.find();
        final Flux<BsonDocument> source = source(documentFindPublisher);
        source.mergeWith(documentFindPublisher);

        Subscriber<BsonDocument> test = new Subscriber<BsonDocument>() {

            List<WriteModel<BsonDocument>> dataList = new ArrayList<>();

            long readNum = 0;
            Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                s.request(128);
            }

            @Override
            public void onNext(BsonDocument document) {
                dataList.add(new InsertOneModel<>(document));
                readNum++;
                if (readNum % 128 == 0) {
                    pushData();
                    subscription.request(128);
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {
                pushData();
            }

            public void pushData() {
                //System.out.println(Thread.currentThread().getName());
                BatchDataEntity<WriteModel<BsonDocument>> dataEntity = new BatchDataEntity<>();
                dataEntity.setDataList(this.dataList);
                dataEntity.setNs(collection.getNamespace().getFullName());
                FullMetaData.getFullMetaData("test").putData(dataEntity);
                this.dataList = new ArrayList<>();
            }
        };
        source.subscribe(test);
    }

    private static void consumeFromBuffer() {
        Flux<BatchDataEntity<WriteModel<BsonDocument>>> dataFlux = Flux.create(sink -> {
            while (true) {
                BatchDataEntity<WriteModel<BsonDocument>> data = FullMetaData.getFullMetaData("test").getData();
                if (data != null) {
                    sink.next(data);
                }
            }
        });

        Consumer<BatchDataEntity<WriteModel<BsonDocument>>> target_test = batchDataEntity -> {
            if (batchDataEntity == null) {
                return;
            } else {
                MongoNamespace mongoNamespace = new MongoNamespace(batchDataEntity.getNs());
                List<WriteModel<BsonDocument>> dataList = batchDataEntity.getDataList();
                MongoCollection<BsonDocument> collection = MongoDBConnectionReactive.getMongoClient("target_test")
                        .getDatabase(mongoNamespace.getDatabaseName())
                        .getCollection(mongoNamespace.getCollectionName(), BsonDocument.class);

                collection.bulkWrite(dataList).subscribe(new Subscriber<BulkWriteResult>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(BulkWriteResult bulkWriteResult) {
                        System.out.println(Thread.currentThread().getName());
                        int insertedCount = bulkWriteResult.getInsertedCount();
                        longAdder.add(insertedCount);
                    }

                    @Override
                    public void onError(Throwable t) {
                        // Handle error
                        t.printStackTrace();
                    }

                    @Override
                    public void onComplete() {
                        // Handle completion
                    }
                });
            }
        };

        final Flux<BatchDataEntity<WriteModel<BsonDocument>>> consumer = dataFlux.subscribeOn(Schedulers.fromExecutor(ThreadPoolManager.getPool("test_target").getExecutorService()));

        consumer.subscribe(target_test);
        consumer.subscribe(target_test);
        consumer.subscribe(target_test);
        consumer.subscribe(target_test);
    }

}
