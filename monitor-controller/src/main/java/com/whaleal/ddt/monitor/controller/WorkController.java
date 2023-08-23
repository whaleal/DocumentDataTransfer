package com.whaleal.ddt.monitor.controller;

import com.whaleal.ddt.monitor.service.MonitorDataService;
import com.whaleal.ddt.monitor.service.WorkService;
import com.whaleal.ddt.monitor.util.R;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/work")
public class WorkController {

    @Autowired
    private WorkService workService;
    @Autowired
    private MonitorDataService monitorDataService;

    /**
     * 获取工作信息列表
     *
     * @param workName 配置信息的任务名称
     * @return 执行结果，不需要分页
     */
    @GetMapping("/getWorkInfoList")
    public R getWorkInfoList(@RequestParam(required = false, defaultValue = "") String workName) {
        return R.ok().put("data", workService.getWorkInfoList(workName));
    }

    /**
     * 获取特定工作信息
     *
     * @param workName 工作名称
     * @return 执行结果
     */
    @GetMapping("/getWorkInfo/{workName}")
    public R getWorkInfo(@PathVariable("workName") String workName) {
        return R.ok().put("data", workService.getWorkInfo(workName));
    }

    /**
     * 获取工作监控信息
     *
     * @param workName   工作名称
     * @param startTime  监控数据起始时间
     * @param endTime    监控数据结束时间
     * @param type       监控数据类型
     * @return 执行结果
     */
    @GetMapping("/getWorkMonitor/{workName}")
    public R getWorkMonitor(@PathVariable("workName") String workName,
                            @RequestParam("startTime") long startTime,
                            @RequestParam("endTime") long endTime,
                            @RequestParam("type") String type) {

        R r = R.ok();
        r.putAll(monitorDataService.getWorkMonitor(workName, startTime, endTime, type));
        return r;
    }
}
