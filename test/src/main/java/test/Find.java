package test;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * @author liheping
 */
public class Find {


    public static void main(String[] args) {

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                find();
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

    public static void find() {
        MongoClient client = MongoClients.create("mongodb://192.168.12.200:24578");
        MongoCollection<Document> collection = client.getDatabase("doc").getCollection("reactive");

        final Subscriber<Document> subscriber = new Subscriber<Document>() {
            private Subscription subscription;
            int totalReadNum = 0;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(100);
            }

            @Override
            public void onNext(Document document) {
                System.out.println(document.toJson());

                if (totalReadNum % 100 == 0) {
                    subscription.request(100);
                }
                totalReadNum++;
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("Failed");

            }

            @Override
            public void onComplete() {
                System.out.println("Completed");
                System.out.println(totalReadNum);
            }
        };

        collection.find(new Document()).subscribe(subscriber);
    }

}
