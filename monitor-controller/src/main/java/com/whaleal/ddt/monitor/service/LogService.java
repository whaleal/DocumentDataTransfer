package com.whaleal.ddt.monitor.service;


import com.whaleal.ddt.monitor.model.LogEntity;

import java.util.List;

/**
 * @author cc
 */
public interface LogService {


    /**
     * 根据条件查询日志
     *
     * @param type      同步类型
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @param info      日志内容
     * @param pageIndex 第几页
     * @param pageSize  每页大小
     * @return 日志数据
     */
    List<LogEntity> findLog(String type, long startTime, long endTime, String info, Integer pageIndex, Integer pageSize);



    void saveLog(LogEntity logEntity);
}
