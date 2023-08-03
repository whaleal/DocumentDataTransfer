import com.mongodb.ConnectionString;
import com.mongodb.Function;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.*;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.concurrent.TimeUnit;

public class MongoDBReader {

    public static void main(String[] args) {
        // 设置MongoDB连接字符串
        ConnectionString connectionString = new ConnectionString("mongodb://192.168.12.200:24578");

        // 配置MongoClientSettings
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();

        // 创建MongoClient
        MongoClient mongoClient = MongoClients.create(settings);

        // 选择数据库和集合
        MongoDatabase database = mongoClient.getDatabase("doc");
        final FindPublisher<Document> lhp6 = database.getCollection("lhp6").find();

        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        Flux.from(lhp6).subscribe(new Subscriber<Document>() {
            private Subscription ss;

            @Override
            public void onSubscribe(Subscription s) {
                System.out.println("init");
                this.ss = s;
                // 请求第一个数据项
                s.request(1);
            }

            @Override
            public void onNext(Document document) {
                // 处理查询结果
                System.out.println(document);

                // 继续请求下一个数据项
                ss.request(1);
            }

            @Override
            public void onError(Throwable t) {
                // 处理错误情况
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                // 查询完成后的处理
                System.out.println("Query completed.");
                // 关闭MongoClient
            }
        });


        Flux.range(1,4).subscribe(new Subscriber<Integer>() {
            Subscription ss ;
            @Override
            public void onSubscribe(Subscription subscription) {
               ss = subscription ;
               ss.request(1);
            }

            @Override
            public void onNext(Integer integer) {
                System.out.println(integer);
                ss.request(1);

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });


    }


}
