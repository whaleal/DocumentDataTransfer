//import com.mongodb.client.MongoClient;
//import org.bson.Document;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * @projectName: mongoT-enterprise
// * @package: PACKAGE_NAME
// * @className: Test
// * @author: Eric
// * @description: TODO
// * @date: 18/07/2023 13:51
// * @version: 1.0
// */
//public class Test {
//
//    public static void main(String[] args) {
//
//
//    }
//
//
//    public static void bucket(MongoClient mongoClient) {
//
//        Runnable runnable = new Runnable() {
//            @Override
//            public void run() {
//                String name = Thread.currentThread().getName();
//                // 生成insert
//                List<Long> batchNum = new ArrayList<>();
//                for (int i = 0; i < 1000; i++) {
//                    batchNum.add(Math.round(Math.random() * 100000));
//                }
//                for (int i = 0; i < 8; i++) {
//                    List<Document> list = new ArrayList<>();
//                    for (int j = 0; j < 100; j++) {
//                        Document document = new Document();
//                        document.put("threadName", name);
//                        document.put("batchNo", batchNum.get(i));
//                        list.add(document);
//                    }
//                    mongoClient.getDatabase("test").getCollection("test").insertMany(list);
//                }
//
//                for (int i = 0; i < 2; i++) {
//                    Document updateQuery = new Document();
//                    updateQuery.put("threadName", name);
//                    updateQuery.put("batchNo", batchNum.get(i));
//                    Document updateValue = new Document();
//                    updateValue.put("$set",new Document().append("updateBy","lhp"));
//                    mongoClient.getDatabase("test").getCollection("test").updateMany(updateQuery,updateValue);
//                }
//            }
//        };
//        new Thread(runnable).start();
//    }
//}
