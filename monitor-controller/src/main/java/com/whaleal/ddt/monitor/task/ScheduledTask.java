package com.whaleal.ddt.monitor.task;


import com.whaleal.ddt.monitor.service.ParseFileLogService;
import com.whaleal.ddt.monitor.service.WorkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;


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
    private static boolean isparse = false;

    @Scheduled(cron = "0/10 * * * * ? ")
    private void checkState() {
        if (!isparse) {
            isparse = true;
            parseFileLogService.readFile("/Users/liheping/Desktop/log.log", 0);
        }

    }


}
