import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

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
        MongoCollection<Document> collection = database.getCollection("lhp6");

        // 执行查询操作
        collection.find(new Document()).subscribe(new Subscriber<Document>() {
            private Subscription s;

            @Override
            public void onSubscribe(Subscription s) {
                this.s = s;
                // 请求第一个数据项
                s.request(1);
            }

            @Override
            public void onNext(Document document) {
                // 处理查询结果
                System.out.println(document);

                // 继续请求下一个数据项
                s.request(1);
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


    }


}
