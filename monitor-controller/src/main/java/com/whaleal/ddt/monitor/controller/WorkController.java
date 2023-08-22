package com.whaleal.ddt.monitor.controller;


import com.whaleal.ddt.monitor.service.WorkService;
import com.whaleal.ddt.monitor.util.R;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/work")
public class WorkController {

    @Autowired
    private WorkService workService;

    /**
     * @param workName 配置信息的任务名称
     * @return 执行结果. 不需要分页
     */
    @GetMapping("/getWorkInfoList")
    public R getWorkInfoList(@RequestParam(required = false, defaultValue = "") String workName) {
        return R.ok().put("data", workService.getWorkInfoList(workName));
    }

    @GetMapping("/getWorkInfo/{workName}")
    public R getWorkInfo(@PathVariable("workName") String workName) {
        return R.ok().put("data", workService.getWorkInfo(workName));
    }


}
