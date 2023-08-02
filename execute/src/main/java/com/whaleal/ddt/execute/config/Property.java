package com.whaleal.ddt.execute.config;


import lombok.extern.log4j.Log4j2;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 配置文件类
 *
 * @author lhp
 * @time 2021-05-31 13:12:12
 */
@Log4j2
public class Property {

    private static Properties pps = new Properties();
    /**
     * 配置文件路径
     */
    private static String fileName = "D2T.properties";
    /**
     * 配置信息K-V
     */
    private static HashMap<String, String> propertiesMap = new HashMap<String, String>();

    public static void setFileName(String fileNameTemp) {
        fileName = fileNameTemp;
        log.info("Read configuration file information:" + fileName);
        readProperties();
    }

    /**
     * readProperties 读取配置文件
     *
     * @desc 读取配置文件
     */
    private static void readProperties() {
        try (FileInputStream inputStream = new FileInputStream(fileName)) {
            pps.load(inputStream);
            Enumeration enum1 = pps.propertyNames();
            while (enum1.hasMoreElements()) {
                String strKey = (String) enum1.nextElement().toString().trim();
                String strValue = pps.getProperty(strKey).trim();
                propertiesMap.put(strKey, strValue);
            }
            log.info(propertiesMap.toString());
        } catch (Exception e) {
            log.error("Failed to read configuration file:fileName:" + fileName + ",exception:" + e.getMessage());
        }
    }

    public static String getPropertiesByKey(String key) {
        //默认返回值为空字符串
        String value = "";
        if (propertiesMap.containsKey(key)) {
            value = propertiesMap.get(key);
        }
        return value.trim();
    }

    public static Map<String, String> getProperties() {
        return propertiesMap;
    }

    public static void saveLastOplogCheckPoint(long oplogTs) {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream("../oplogTs.txt");
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
            bufferedWriter.write(oplogTs + "");
            bufferedWriter.close();
            fileOutputStream.close();
        } catch (Exception e) {
            log.error("Exception occurred while saving oplogTs,msg:" + e.getMessage());
        }
    }

    public static void setProperties(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            propertiesMap.put(entry.getKey(), entry.getValue().toString());
        }
    }

    public static void main(String[] args) {
        saveLastOplogCheckPoint(System.currentTimeMillis());
    }
}
