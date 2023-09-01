package test;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.LongAdder;

public class MultiTableMultiThreadMongoReactiveExample {
    private static LongAdder longAdder=new LongAdder();

    public static void main(String[] args) {
        ConnectionString connectionString = new ConnectionString("mongodb://192.168.12.200:24578");

        MongoDatabase database = MongoClients.create(connectionString).getDatabase("source");

        BlockingQueue<Document> buffer = new LinkedBlockingQueue<>(100);

        readAndPublishFromCollection(database, "lhp6", buffer);
//        readAndPublishFromCollection(database, "lhp7", buffer);
        // Add more collections as needed

        consumeFromBuffer(buffer);
        while (true){
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(longAdder.sum());
        }
    }

    private static void readAndPublishFromCollection(MongoDatabase database, String collectionName, BlockingQueue<Document> buffer) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        final FindPublisher<Document> documentFindPublisher = collection.find();

        Flux<Document> documentFlux = Flux.from(documentFindPublisher)
                .subscribeOn(Schedulers.newParallel("reader-" + collectionName, 10));

        documentFlux.subscribe(document -> {
            try {
                buffer.put(document);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private static void consumeFromBuffer(BlockingQueue<Document> buffer) {
        Flux.from(Mono.fromCallable(buffer::take).repeat())
                .subscribeOn(Schedulers.newParallel("consumer", 10))
                .subscribe(document -> {
                    longAdder.add(1);
                    // Add your processing logic here
                });
    }
}
