package com.whaleal.ddt.monitor.task;



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


    @Scheduled(cron = "0 0/1 * * * ? ")
    private void checkState() {

    }




}
