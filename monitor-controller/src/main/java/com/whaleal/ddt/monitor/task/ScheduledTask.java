package com.whaleal.ddt.monitor.task;


import com.whaleal.ddt.monitor.service.ParseFileLogService;
import com.whaleal.ddt.monitor.service.WorkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author liheping
 */
@Configuration
@EnableScheduling
public class ScheduledTask {

    @Autowired
    private WorkService taskService;

    @Autowired
    private ParseFileLogService parseFileLogService;

    private static final Object syncLock = new HashMap<>();

    private static volatile long filePointerTemp = 0L;

    private static AtomicBoolean isReadIng = new AtomicBoolean(false);

    /**
     * 10s读取一次文件
     */
    //@Scheduled(cron = "0/10 * * * * ? ")
    private void checkState() {
        // 如果读取不完怎么办
        Runnable runnable = () -> {
            if (!isReadIng.get() && isReadIng.compareAndSet(false, true)) {
                try {
                    // todo 可以设置参数
                    File file = new File("../logs/log.log");
                    if (file.exists() && file.isFile()) {
                        if (file.length() >= filePointerTemp) {
                            filePointerTemp = parseFileLogService.readFile(file.getAbsolutePath(), filePointerTemp);
                        } else {
                            filePointerTemp = parseFileLogService.readFile(file.getAbsolutePath(), 0);
                        }
                    }
                } finally {
                    isReadIng.set(false);
                }
            }
        };
        new Thread(runnable).start();
    }

    private static volatile boolean isRead = false;

    @Scheduled(cron = "0/10 * * * * ? ")
    private void test() {
        if (!isRead) {
            isRead = true;
            parseFileLogService.readFile("/Users/liheping/Desktop/log.log", 0);
        }

    }
}
