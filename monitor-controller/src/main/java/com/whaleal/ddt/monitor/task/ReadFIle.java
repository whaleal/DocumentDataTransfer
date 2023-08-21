package com.whaleal.ddt.monitor.task;

import com.alibaba.fastjson.JSON;
import com.whaleal.ddt.monitor.model.LogEntity;
import com.whaleal.ddt.monitor.util.DateTimeUtils;
import com.whaleal.icefrog.core.util.StrUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @projectName: full-common
 * @package: com.whaleal.ddt.monitor.task
 * @className: ReadFIle
 * @author: Eric
 * @description: TODO
 * @date: 21/08/2023 16:18
 * @version: 1.0
 */
public class ReadFIle {


    public static void main(String[] args) {
        readFile();
    }

    public static void readFile() {
        File file = new File("/Users/liheping/Desktop/log.log");


        String workName="";

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.split(" +", 8);
                LogEntity logEntity = new LogEntity();
                if (split.length < 8) {
                    continue;
                }
                logEntity.setTime(DateTimeUtils.stringToStamp(split[0] + " " + split[1]));
                logEntity.setProcessId(split[2]);
                logEntity.setType(split[5]);
                logEntity.setInfo(split[7]);
                // 表示一个新的任务出现了
                if(logEntity.getInfo().startsWith("enable Start task ")){



                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
