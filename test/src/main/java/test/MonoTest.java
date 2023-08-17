package test;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

class MonoTest {





    static void find() {


        MongoClient client= MongoClients.create("mongodb://192.168.12.200:24578");
        MongoCollection<Document> collection = client.getDatabase("log").getCollection("D2TCpu");
        FindPublisher<Document> documentFindPublisher = collection.find();

        System.out.println("=====================");

        Flux<Document> documentFlux = Flux.from(documentFindPublisher);
        documentFlux.subscribe(document -> {
            System.out.println("Query Result: " + document.toJson());
        });

        System.out.println("=====================");
        // 等待查询操作完成
        Mono<Void> queryResult = documentFlux.then();
        queryResult.block();
    }



    void write(){
        MongoClient client= MongoClients.create("mongodb://192.168.12.200:24578");
        MongoCollection<Document> collection = client.getDatabase("target").getCollection("log");
        List<Document> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Document document = new Document();
            document.append("age",18);
            document.append("name","张"+i);
            document.append("address","china");
            list.add(document);
        }


//        collection.bulkWrite(list);
    }

    void insert(){

        MongoClient client= MongoClients.create("mongodb://192.168.12.200:24578");
        MongoCollection<Document> collection = client.getDatabase("target").getCollection("log");

        // 构建插入文档
        Document doc1 = new Document("key1", "value1");
        Document doc2 = new Document("key2", "value2");

        // 创建WriteModel集合
        List<WriteModel<Document>> writes = new ArrayList<>();
        writes.add(new InsertOneModel<>(doc1));
        writes.add(new InsertOneModel<>(doc2));

        // 执行bulkWrite操作
        Mono<BulkWriteResult> bulkWriteResultMono = (Mono<BulkWriteResult>) collection.bulkWrite(writes);

        // 使用block方法获取执行结果（非阻塞）
        BulkWriteResult bulkWriteResult = bulkWriteResultMono.block();

        if (bulkWriteResult != null) {
            System.out.println("Inserted Count: " + bulkWriteResult.getInsertedCount());
            System.out.println("Modified Count: " + bulkWriteResult.getModifiedCount());
            System.out.println("Matched Count: " + bulkWriteResult.getMatchedCount());
        }

        // 关闭MongoClient
//        mongoClient.close();


    }


    static void block(){
        MongoClient client= MongoClients.create("mongodb://192.168.12.200:24578");
        MongoCollection<Document> collection = client.getDatabase("target").getCollection("target");
        // 构建插入文档
        Document doc1 = new Document("key1", "value1");
        Document doc2 = new Document("key2", "value2");

        // 创建WriteModel集合
        List<WriteModel<Document>> writes = new ArrayList<>();
        writes.add(new InsertOneModel<>(doc1));
        writes.add(new InsertOneModel<>(doc2));

        // 执行bulkWrite操作
        Mono<BulkWriteResult> bulkWriteResultMono = (Mono<BulkWriteResult>) collection.bulkWrite(writes);

        // 订阅并处理结果
        bulkWriteResultMono.subscribe(bulkWriteResult -> {
            System.out.println("===========================");
            System.out.println("Inserted Count: " + bulkWriteResult.getInsertedCount());
            System.out.println("Modified Count: " + bulkWriteResult.getModifiedCount());
            System.out.println("Matched Count: " + bulkWriteResult.getMatchedCount());
            System.out.println("===========================");
        });

        // 等待操作完成
        bulkWriteResultMono.block();
    }


    void back(){

        MongoClient client= MongoClients.create("mongodb://192.168.12.200:24578");
        MongoCollection<Document> collection = client.getDatabase("target").getCollection("target");

        // 创建WriteModel集合
        List<WriteModel<Document>> writes = new ArrayList<>();

        for (int i = 0; i < 1000000; i++) {
            Document document = new Document();
            document.append("age",18);
            document.append("name","张"+i);
            document.append("address","china");
            writes.add(new InsertOneModel<>(document));
        }

        System.out.println("开始插入");

        // 执行bulkWrite操作
        Mono<BulkWriteResult> bulkWriteResultMono = (Mono<BulkWriteResult>) collection.bulkWrite(writes);

        // 订阅并处理结果（后台订阅）
        bulkWriteResultMono.subscribe(bulkWriteResult -> {
            System.out.println("===========================");
            System.out.println("Inserted Count: " + bulkWriteResult.getInsertedCount());
            System.out.println("Modified Count: " + bulkWriteResult.getModifiedCount());
            System.out.println("Matched Count: " + bulkWriteResult.getMatchedCount());
            System.out.println("===========================");
        });
        // 不阻塞等待操作完成
    }

    public static void main(String[] args) {
        find();
    }

}
