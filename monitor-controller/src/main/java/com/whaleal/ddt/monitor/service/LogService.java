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
     * @param processId 进程Id
     * @param type      同步类型
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @param info      日志内容
     * @param pageIndex 第几页
     * @param pageSize  每页大小
     * @return 日志数据
     */
    List<LogEntity> findLog(String processId, String type, long startTime, long endTime, String info, Integer pageIndex, Integer pageSize);

    /**
     * 获取条件查询日志数
     *
     * @param processId 进程Id
     * @param type      同步类型
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @param info      日志内容
     * @return 日志条数
     */
    long findLogCount(String processId, String type, long startTime, long endTime, String info);
}
