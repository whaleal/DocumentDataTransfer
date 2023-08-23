package com.whaleal.ddt.monitor.service;

import java.util.List;
import java.util.Map;

public interface WorkService {

    /**
     * 插入或更新工作信息
     *
     * @param workName  工作名称
     * @param workInfo  工作信息
     */
    void upsertWorkInfo(String workName, Map<Object, Object> workInfo);

    /**
     * 获取特定工作信息
     *
     * @param workName  工作名称
     * @return 工作信息
     */
    Map<Object, Object> getWorkInfo(String workName);

    /**
     * 获取工作信息列表
     *
     * @param workName  工作名称
     * @return 工作信息列表
     */
    List<Map<Object, Object>> getWorkInfoList(String workName);
}
