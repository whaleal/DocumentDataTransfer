/*
 * MongoT - An open-source project licensed under GPL+SSPL
 *
 * Copyright (C) [2023 - present ] [Whaleal]
 *
 * This program is free software; you can redistribute it and/or modify it under the terms
 * of the GNU General Public License and Server Side Public License (SSPL) as published by
 * the Free Software Foundation; either version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License and SSPL for more details.
 *
 * For more information, visit the official website: [www.whaleal.com]
 */
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
