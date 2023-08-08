//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoClients;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoCursor;
//import com.mongodb.client.model.InsertOneModel;
//import com.mongodb.client.model.WriteModel;
//import lombok.extern.log4j.Log4j2;
//import org.bson.BsonDocument;
//import org.bson.Document;
//import org.bson.RawBsonDocument;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * @projectName: DocumentDataTransfer
// * @package: PACKAGE_NAME
// * @className: BsonTest
// * @author: Eric
// * @description:
// * @date: 02/08/2023 13:49
// * @version: 1.0
// */
//@Log4j2
//public class BsonTest {
//    static {
//        log.info("\n\n\n" +
//                "  ____    ____    _____ \n" +
//                " |  _ \\  |___ \\  |_   _|\n" +
//                " | | | |   __) |   | |  \n" +
//                " | |_| |  / __/    | |  \n" +
//                " |____/  |_____|   |_|  \n" +
//                "                        ");
//    }
//    public static void main(String[] args) {
//        final MongoClient mongoClient = MongoClients.create("mongodb://192.168.12.100:24578");
//
//        final MongoCollection<Document> collection = mongoClient.getDatabase("doc").getCollection("lhp6");
//        final MongoCursor<BsonDocument> cursor = collection.find(new BsonDocument(), RawBsonDocument.class).cursor();
//
//        while (cursor.hasNext()) {
//            RawBsonDocument next = cursor.next();
//            InsertOneModel<RawBsonDocument> model=new InsertOneModel<>(next);
//            List<WriteModel> modelList=new ArrayList<>();
//            modelList.add(model);
//
//            mongoClient.getDatabase("doc").getCollection("lhp6_bak").bulkWrite(modelList);
//        }
//    }
//}
