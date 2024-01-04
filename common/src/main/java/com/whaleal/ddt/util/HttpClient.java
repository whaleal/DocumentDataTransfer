package com.whaleal.ddt.util;

import com.alibaba.fastjson2.JSON;
import com.whaleal.icefrog.core.util.StrUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Log4j2
public class HttpClient {

    public static void main(String[] args) {
        String url = "http://localhost:9600/api/collection/ddt/saveDDTInfo/taskname";
        Map<String, Object> ddtInfo = new HashMap<>();

        saveDDTInfo(url, JSON.toJSONString(ddtInfo));
    }

    public static void saveDDTInfo(String url, String jsonString) {
        if (StrUtil.isBlank(url) || StrUtil.isBlank(jsonString)) {
            return;
        }

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost post = new HttpPost(url);
        CloseableHttpResponse response = null;
        try {
            post.setHeader("Content-Type", "application/json");
            if (null != jsonString) {
                post.setEntity(new ByteArrayEntity(jsonString.getBytes(StandardCharsets.UTF_8)));
            }
            log.info("url:{},data:{}", url, jsonString);
            response = httpClient.execute(post);
            if (response != null && response.getStatusLine().getStatusCode() == 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                log.info("result:{}", responseBody);
            } else {
                log.info("result:{}", "api error");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static String getClusterAuthInfo(String url) {
        if (StrUtil.isBlank(url)) {
            return "";
        }
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet get = new HttpGet(url);
        CloseableHttpResponse response = null;
        try {
            get.setHeader("Content-Type", "application/json");
            response = httpClient.execute(get);
            if (response != null && response.getStatusLine().getStatusCode() == 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                log.info("result:{}", responseBody);
                return responseBody;
            } else {
                log.info("result:{}", "api error");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "";
    }

    public static void getCertificate(String url, String clusterId, int type) {
        if (StrUtil.isBlank(url)) {
            return;
        }
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet get = new HttpGet(url);
        CloseableHttpResponse response = null;
        try {
            get.setHeader("Content-Type", "application/json");
            response = httpClient.execute(get);
            if (response != null && response.getStatusLine().getStatusCode() == 200) {
                InputStream inputStream = response.getEntity().getContent();

                File file = null;
                if (type == 2) {
                    file = new File("/opt/" + clusterId + "/server.pem");
                } else if (type == 3) {
                    file = new File("/opt/" + clusterId + "/client.pem");
                } else if (type == 4) {
                    file = new File("/opt/" + clusterId + "/ca.crt");
                }
                file.getParentFile().mkdirs();
                file.createNewFile();

                FileOutputStream fileOutputStream = new FileOutputStream(file);

                byte[] buffer = new byte[1024];
                int bytesRead;

                // 从输入流读取内容并写入输出流
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    fileOutputStream.write(buffer, 0, bytesRead);
                }
                // 关闭流
                inputStream.close();
                fileOutputStream.close();
            } else {
                log.info("result:{}", "api error");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
