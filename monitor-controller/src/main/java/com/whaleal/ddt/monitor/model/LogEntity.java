package com.whaleal.ddt.monitor.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author cc
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class LogEntity {
    /**
     * Id
     */
    private String id;

    /**
     * 进程ID
     */
    private String processId;

    /**
     * 日志时间 暂不考虑时区问题
     */
    private long time;
    /**
     * 日志类型
     */
    private String type;

    /**
     * 日志信息
     */
    private String info;

}
