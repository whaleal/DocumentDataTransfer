package com.whaleal.ddt.util;

import com.alibaba.fastjson2.JSON;
import com.whaleal.icefrog.core.util.StrUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Log4j2
public class HttpClientPost {

    public static void main(String[] args) {
        String url = "http://localhost:9600/api/collection/ddt/saveDDTInfo/taskname";
        Map<String, Object> ddtInfo = new HashMap<>();

        postJson(url, JSON.toJSONString(ddtInfo));
    }

    public static void postJson(String url, String jsonString) {
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



    public static void getCertificate(){

    }
}
