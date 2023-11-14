package com.whaleal.ddt.buckup;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.connection.ServerDescription;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Log4j2
/**
 * @projectName: full-common
 * @package: com.whaleal.ddt.buckup
 * @className: WriteData
 * @author: Eric
 * @description: TODO
 * @date: 09/11/2023 14:54
 * @version: 1.0
 */
public class WriteData {
    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create(args[0]);
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("test");
        int loopy = 0;
        while (true) {
            try {
                loopy++;
                if (loopy >= 10) {
                    loopy = 0;
                    TimeUnit.SECONDS.sleep(1);
                    log.info("-----------------------------");
                    log.info("开始打印集群信息");
                    log.info("repl name" + mongoClient.getClusterDescription().getClusterSettings().getRequiredReplicaSetName());
                    List<ServerDescription> serverDescriptions = mongoClient.getClusterDescription().getServerDescriptions();
                    ServerDescription serverDescription1 = serverDescriptions.get(0);
                    log.info("hosts" + serverDescription1.getHosts());
                    log.info("-----------------------------");
                }

                Document complexDocument = new Document("name", "John Doe")
                        .append("age", 30)
                        .append("address", new Document("city", "New York")
                                .append("state", "NY"))
                        .append("languages", Arrays.asList("Java", "Python", "JavaScript"));
                // 插入数据
                collection.insertOne(complexDocument);

                collection.insertOne(new Document("int1", 1)
                        .append("int2", 2)
                        .append("int3", 3)
                        .append("int4", 4)
                        .append("int5", 5)
                        .append("int6", 6)
                        .append("int7", 7)
                        .append("int8", 8)
                );
//             创建一个Document对象用于插入数据
                Document document = new Document("name", "John Doe")
                        .append("age", 30)
                        .append("email", "john.doe@example.com");
                // 插入数据
                collection.insertOne(document);
                System.out.println("Inserted document");
                // 查询数据
                FindIterable<Document> documents = collection.find(new Document("name", "John Doe"));
                for (Document doc : documents) {
                    System.out.println(doc.toJson());
                }
                // 更新数据
                collection.updateOne(Filters.eq("name", "John Doe"), new Document("$set", new Document("age", 31)));
                System.out.println("Updated document");
                // 删除数据
                collection.deleteOne(Filters.eq("name", "John Doe"));
                System.out.println("Deleted document");
                // 模拟慢查询
                Document aggregateList = new Document("$group", new Document("_id", "$host")
                        .append("int1_avg", new Document("$avg", "$int1"))
                        .append("int2_avg", new Document("$avg", "$int2"))
                        .append("int3_avg", new Document("$avg", "$int3"))
                        .append("int4_avg", new Document("$avg", "$int4"))
                        .append("int5_avg", new Document("$avg", "$int5"))
                        .append("int6_avg", new Document("$avg", "$int6"))
                        .append("int7_avg", new Document("$avg", "$int7"))
                        .append("int8_avg", new Document("$avg", "$int8"))
                        .append("int9_avg", new Document("$avg", "$int9"))
                        .append("int10_avg", new Document("$avg", "$int10"))
                );
                List<Document> documentList = Arrays.asList(aggregateList);
                AggregateIterable<Document> aggregate = collection.aggregate(documentList).allowDiskUse(true);
                for (Document document1 : aggregate) {
                    System.out.println(document1);
                }
            } catch (Exception e) {
                e.printStackTrace();

            }
        }
    }
}
