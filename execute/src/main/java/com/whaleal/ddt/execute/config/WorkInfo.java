package com.whaleal.ddt.execute.config;


import com.whaleal.ddt.util.HostInfoUtil;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


/**
 * @author: lhp
 * @time: 2021/7/22 2:00 下午
 * @desc: 配置文件类
 */
@Data
@NoArgsConstructor
public class WorkInfo implements Cloneable, Serializable {

    public static final String SYNC_MODE_ALL = "all";
    public static final String SYNC_MODE_REAL_TIME = "realTime";
    public static final String SYNC_MODE_ALL_AND_REAL_TIME = "allAndRealTime";
    public static final String SYNC_MODE_ALL_AND_INCREMENT = "allAndIncrement";


    /**
     * 任务名称
     */
    private String workName;
    /**
     * 源端数据源url
     */
    private String sourceDsUrl;
    /**
     * 目标数据源url
     */
    private String targetDsUrl;
    /**
     * 同步模式
     * 全量:all
     * 实时:realTime
     * 全量加增量:allAndRealTime
     */
    private String syncMode;
    /**
     * 表过滤正则
     */
    private String dbTableWhite;
    /**
     * 需同步的ddl集合
     * set中包含*号 则全部ddl都要同步
     */
    private Set<String> ddlFilterSet;

    /**
     * 全量同步时
     * sourceThreadNum 读取任务线程个数
     * targetThreadNum 写入任务线程个数
     * createIndexThreadNum 建立索引线程个数
     */
    private int sourceThreadNum = Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F);
    private int targetThreadNum = Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F);
    private int createIndexThreadNum = Math.round(HostInfoUtil.computeTotalCpuCore());

    /**
     * 实时同步时
     * 实时任务总的线程数
     */
    private int realTimeThreadNum = Math.round(HostInfoUtil.computeTotalCpuCore() * 2);

    /**
     * ddl处理超时参数 单位秒 默认600s
     */
    private int ddlWait = 1200;

    /**
     * 每个缓存区缓存批次数量
     */
    private int bucketSize;
    /**
     * 缓存区个数
     */
    private int bucketNum;
    /**
     * 每批次数据的大小
     */
    private int batchSize;
    /**
     * 增量同步或实时同步时。时间戳格式，单位s
     * oplog的开始时间
     */
    private int startOplogTime = (int) (System.currentTimeMillis() / 1000);
    /**
     * 增量同步或实时同步时。时间戳格式，单位s
     * oplog的结束时间
     */
    private int endOplogTime = 0;
    /**
     * 延迟时间 单位秒
     */
    private int delayTime;
    /**
     * 程序启动时间
     */
    private volatile long startTime = System.currentTimeMillis();
    /**
     * 同步DDL信息
     */
    private Set<String> clusterInfoSet = new HashSet<>();

    public void setClusterInfoSet(Set<String> clusterInfoSet) {
        this.clusterInfoSet.addAll(clusterInfoSet);
    }
}
