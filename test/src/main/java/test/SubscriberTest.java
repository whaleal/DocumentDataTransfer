package test;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;

import org.bson.Document;

import util.SubscriberHelpers;

import java.util.ArrayList;
import java.util.List;

public class SubscriberTest {

    static MongoClient client = MongoClients.create("mongodb://192.168.12.200:24578");
    static MongoCollection<Document> collection = client.getDatabase("target").getCollection("target");

    static void findAll() {

        SubscriberHelpers.PrintToStringSubscriber<Document> subscriber = new SubscriberHelpers.PrintToStringSubscriber<>();
        collection.find().subscribe(subscriber);

        subscriber.await();
        System.out.println("===================");
        subscriber.get().forEach(Document::toString);
        System.out.println("===================");
    }



    void insertMany() {

        List<WriteModel<Document>> writes = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            Document document = new Document();
            document.append("age", 18);
            document.append("name", "张" + i);
            document.append("address", "china");
            writes.add(new InsertOneModel<>(document));
        }

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                SubscriberHelpers.ObservableSubscriber<BulkWriteResult> subscriber = new SubscriberHelpers.OperationSubscriber<>();
                collection.bulkWrite(writes).subscribe(subscriber);
                subscriber.await();
                System.out.println("=====================");
                List<BulkWriteResult> x = subscriber.get();
                System.out.println(x);
            }
        };

        new Thread(runnable).start();


        System.out.println("命令执行结束 ");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        findAll();
    }


}
