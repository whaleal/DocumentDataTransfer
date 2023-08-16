package com.whaleal.ddt.execute.realtime.common;

import com.whaleal.ddt.common.Datasource;
import com.whaleal.ddt.execute.realtime.RealTimeChangeStream;
import com.whaleal.ddt.execute.realtime.RealTimeOplog;
import com.whaleal.ddt.execute.config.WorkInfo;
import com.whaleal.ddt.realtime.common.cache.MetaData;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.sync.connection.MongoDBConnectionSync;
import com.whaleal.ddt.thread.pool.ThreadPoolManager;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.TimeUnit;

/**
 * @projectName: DocumentDataTransfer
 * @package: com.whaleal.ddt.execute.common
 * @className: RealTimeWork
 * @author: Eric
 * @description: TODO
 * @date: 15/08/2023 14:02
 * @version: 1.0
 */
@Log4j2
public abstract class RealTimeWork {
    /**
     * 工作名称
     */
    protected final String workName;
    /**
     * 源数据源名称
     */
    protected final String sourceDsName;
    /**
     * 目标数据源名称
     */
    protected final String targetDsName;
    /**
     * 读取Oplog的线程池名称
     */
    protected final String readOplogThreadPoolName;
    /**
     * 解析NS的线程池名称
     */
    protected final String parseNSThreadPoolName;
    /**
     * NS Bucket Oplog的线程池名称
     */
    protected final String nsBucketOplogThreadPoolName;
    /**
     * 写入目标数据的线程池名称
     */
    protected final String writeThreadPoolName;


    protected RealTimeWork(String workName) {
        this.workName = workName;
        // 数据源名称
        this.sourceDsName = workName + "_source";
        this.targetDsName = workName + "_target";
        // 各种任务数据源名称
        this.readOplogThreadPoolName = workName + "_readOplogThreadPoolName";
        this.parseNSThreadPoolName = workName + "_parseNSThreadPoolName";
        this.nsBucketOplogThreadPoolName = workName + "_nsBucketOplogThreadPoolName";
        this.writeThreadPoolName = workName + "_writeThreadPoolName";
    }

    /**
     * 初始化方法，建立连接到源数据源和目标数据源，并根据给定的总线程数计算出用于读取oplog、解析ns、分桶操作和写入的线程数量。
     * 然后创建相应的线程池。
     *
     * @param sourceDsUrl       源数据源的连接URL
     * @param targetDsUrl       目标数据源的连接URL
     * @param nsBucketThreadNum 分桶操作的线程数
     * @param writeThreadNum    写入目标数据的线程数
     */
    public void init(String sourceDsUrl, String targetDsUrl, int nsBucketThreadNum, int writeThreadNum) {
        // 建立连接 放在外部处理
        initConnection(sourceDsName, sourceDsUrl);
        initConnection(targetDsName, targetDsUrl);
        // 计算bucket 和 write部分的线程个数
        // 初始化线程次
        intiThreadPool(readOplogThreadPoolName, 1);
        intiThreadPool(parseNSThreadPoolName, 1);
        // 这个为系统自动生成的线程信息
        intiThreadPool(nsBucketOplogThreadPoolName, nsBucketThreadNum);
        intiThreadPool(writeThreadPoolName, writeThreadNum);
    }


    /**
     * 初始化MongoDB连接的方法，通过给定的数据源名称(dsName)和URL(url)创建MongoDB连接。
     *
     * @param dsName 数据源名称
     * @param url    数据源连接URL
     */
    private void initConnection(String dsName, String url) {
        MongoDBConnectionSync.createMonoDBClient(dsName, new Datasource(url));
    }

    /**
     * 初始化线程池的方法，通过给定的线程池名称(threadPoolName)和核心线程数(corePoolSize)创建一个线程池。
     *
     * @param threadPoolName 线程池名称
     * @param corePoolSize   线程池的核心线程数
     */
    private void intiThreadPool(String threadPoolName, int corePoolSize) {
        ThreadPoolManager manager = new ThreadPoolManager(threadPoolName, corePoolSize, corePoolSize, Integer.MAX_VALUE);
    }

    /**
     * 创建任务并提交到指定的线程池。该方法接收线程池名称(threadPoolName)和要执行的任务(runnable)作为参数。
     *
     * @param threadPoolName 线程池名称
     * @param runnable       要执行的任务
     */
    public void createTask(String threadPoolName, Runnable runnable) {
        // 提交任何类型的任务
        ThreadPoolManager.submit(threadPoolName, runnable);
    }

    public abstract void submitTask(WorkInfo workInfo, int nsBucketThreadNum, int writeThreadNum);


    /**
     * 打印线程信息的方法。输出当前活动线程数量，包括读取oplog的线程、解析ns的线程、分桶操作的线程和写入的线程。
     */
    public void printThreadInfo() {
        String threadInfo = "{} the current number of {} threads:{}";
        log.info(threadInfo, workName, readOplogThreadPoolName, ThreadPoolManager.getActiveThreadNum(readOplogThreadPoolName));
        log.info(threadInfo, workName, parseNSThreadPoolName, ThreadPoolManager.getActiveThreadNum(parseNSThreadPoolName));
        log.info(threadInfo, workName, nsBucketOplogThreadPoolName, ThreadPoolManager.getActiveThreadNum(nsBucketOplogThreadPoolName));
        log.info(threadInfo, workName, writeThreadPoolName, ThreadPoolManager.getActiveThreadNum(writeThreadPoolName));
    }

    /**
     * 判断实时同步是否完成的方法。通过检查读取oplog的线程数是否为0，来判断是否完成实时同步。
     *
     * @return 如果实时同步已完成，返回true；否则返回false
     */


    public boolean judgeRealTimeTaskFinish() {
        // 当不进行读取的时候 可以任务任务已经中断
        // Q: 但是当读取完成后 未写入的数据怎么办？ 增量情况会缺少数据
        // A：执行 MetadataOplog.getOplogMetadata(workName).waitCacheExe()
        if (ThreadPoolManager.getActiveThreadNum(readOplogThreadPoolName) == 0) {
            // 等待缓存中的数据写完
            MetaData.getMetaData(workName).waitCacheExe();
            // 等待缓存为空
            try {
                TimeUnit.MINUTES.sleep(1);
                // 如果发现关机 则睡眠1分钟 使数据写入到磁盘里面
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    /**
     * 销毁实时同步任务的方法。关闭MongoDB连接池和线程池，释放资源。
     */

    public void destroy() {
        // 清除gc
        //
        // 关闭连接池
        MongoDBConnectionSync.close(sourceDsName);
        MongoDBConnectionSync.close(targetDsName);
        // 关闭线程池
        ThreadPoolManager.destroy(readOplogThreadPoolName);
        ThreadPoolManager.destroy(parseNSThreadPoolName);
        ThreadPoolManager.destroy(nsBucketOplogThreadPoolName);
        ThreadPoolManager.destroy(writeThreadPoolName);
    }


    /**
     * 启动实时同步任务
     *
     * @param workInfo 工作信息
     */
    public static void startRealTime(final WorkInfo workInfo, final String realTimeType) {
        Runnable runnable = () -> {
            log.info("enable Start task :{}, task configuration information :{}", workInfo.getWorkName(), workInfo.toString());
            // 设置程序状态为运行中
            WorkStatus.updateWorkStatus(workInfo.getWorkName(), WorkStatus.WORK_RUN);
            // 缓存区对线
            int maxQueueSizeOfOplog = workInfo.getBucketNum() * workInfo.getBucketSize() * workInfo.getBucketSize();
            MetaData metadataOplog = new MetaData(workInfo.getWorkName(), workInfo.getDdlWait(), maxQueueSizeOfOplog, workInfo.getBucketNum(), workInfo.getBucketSize());
            RealTimeWork realTimeWork = null;
            // 创建实时同步任务对象
            if ("changestream".equals(realTimeType)) {
                realTimeWork = new RealTimeChangeStream(workInfo.getWorkName());
            } else {
                realTimeWork = new RealTimeOplog(workInfo.getWorkName());
            }
            // 初始化任务，连接源数据库和目标数据库
            realTimeWork.init(workInfo.getSourceDsUrl(), workInfo.getTargetDsUrl(), workInfo.getNsBucketThreadNum(), workInfo.getWriteThreadNum());
            // 创建写入任务
            realTimeWork.submitTask(workInfo, workInfo.getNsBucketThreadNum(), workInfo.getWriteThreadNum());
            long executeCountOld = 0L;
            while (true) {
                try {
                    // 每隔10秒输出一次信息
                    TimeUnit.SECONDS.sleep(10);
                    // 输出线程运行情况
                    realTimeWork.printThreadInfo();
                    // 输出缓存区运行情况
                    executeCountOld = metadataOplog.printCacheInfo(workInfo.getStartTime(), executeCountOld);
                    // 判断任务是否结束，如果结束则等待1分钟后退出循环
                    if (realTimeWork.judgeRealTimeTaskFinish()) {
                        WorkStatus.updateWorkStatus(workInfo.getWorkName(), WorkStatus.WORK_STOP);
                        TimeUnit.MINUTES.sleep(1);
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // 回收资源
            realTimeWork.destroy();
        };
        Thread thread = new Thread(runnable);
        thread.setName(workInfo.getWorkName() + "_execute");
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
