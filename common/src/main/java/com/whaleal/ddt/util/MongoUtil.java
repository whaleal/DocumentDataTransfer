package com.whaleal.ddt.util;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.whaleal.ops.common.utils.BouncyCastleUtil;
import org.bson.Document;

import javax.net.ssl.SSLContext;

/**
 * @projectName: DocumentDataTransfer
 * @package: com.whaleal.ddt.util
 * @className: MongoUtil
 * @author: Eric
 * @description: TODO
 * @date: 04/01/2024 17:14
 * @version: 1.0
 */
public class MongoUtil {
    public static MongoClientSettings getMongoClientSettings(String wapGateWay, String clusterId) {
        MongoClientSettings mongoClientSettings = null;
        String clusterAuthInfoStr = HttpClient.getClusterAuthInfo(wapGateWay + "/api/collection/ddt/getClusterAuthInfo" + clusterId);
        Document document = Document.parse(clusterAuthInfoStr);
        Document clusterAuthInfo = document.get("data", Document.class);
        if ("2".equals(clusterAuthInfo.get("authLevel").toString())) {
            HttpClient.getCertificate(wapGateWay + "/api/collection/file/download/certificate?clusterId=" + clusterId, clusterId, 2);
            HttpClient.getCertificate(wapGateWay + "/api/collection/file/download/certificate?clusterId=" + clusterId, clusterId, 3);
            HttpClient.getCertificate(wapGateWay + "/api/collection/file/download/certificate?clusterId=" + clusterId, clusterId, 4);
            try {
                final SSLContext sslContext = BouncyCastleUtil.initContext("/data/" + clusterId + "/ca.crt", "/data/" + clusterId + "/client.pem");
                mongoClientSettings = MongoClientSettings.builder()
                        .applyToSslSettings(builder -> {
                            builder.enabled(true);
                            builder.context(sslContext);
                            builder.invalidHostNameAllowed(true);
                        }).applyConnectionString(new ConnectionString(clusterAuthInfo.get("clusterUrl").toString()))
                        .build();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return mongoClientSettings;
        }
        mongoClientSettings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(clusterAuthInfo.get("clusterUrl").toString())).build();
        return mongoClientSettings;
    }
}
