package test;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;

/**
 * @author liheping
 */
public class BulkWrite {


    public static void main(String[] args) {

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                write();
            }
        };
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Thread thread = new Thread(runnable);
        thread.start();
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void write() {
        MongoClient client = MongoClients.create("mongodb://192.168.12.200:24578");
        MongoCollection<Document> collection = client.getDatabase("doc").getCollection("reactive");
        final ArrayList<WriteModel<Document>> writeModels = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            writeModels.add(new InsertOneModel<>(new Document()));
            writeModels.add(new InsertOneModel<>(new Document()));
            writeModels.add(new InsertOneModel<>(new Document()));
        }


        final Subscriber<BulkWriteResult> subscriber = new Subscriber<BulkWriteResult>() {
            private Subscription subscription;
            int readNum = 0;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(100);
            }

            @Override
            public void onNext(BulkWriteResult bulkWriteResult) {
                final int insertedCount = bulkWriteResult.getInsertedCount();
                System.out.println(insertedCount);
                subscription.request(100);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("Throwable");
            }

            @Override
            public void onComplete() {
                System.out.println("Printing completed.");
            }

            public int getReadNum() {
                return readNum;
            }

        };
        collection.bulkWrite(writeModels).subscribe(subscriber);

    }

}
