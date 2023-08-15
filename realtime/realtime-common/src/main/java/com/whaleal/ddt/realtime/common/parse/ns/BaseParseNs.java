package com.whaleal.ddt.realtime.common.parse.ns;

import com.mongodb.client.MongoClient;
import com.whaleal.ddt.realtime.common.cache.MetaData;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.sync.connection.MongoDBConnectionSync;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @projectName: realtime-sync-oplog
 * @package: com.whaleal.ddt.realtime.common.parse
 * @className: BaseParseNs
 * @author: Eric
 * @description: TODO
 * @date: 15/08/2023 15:59
 * @version: 1.0
 */
@Log4j2
public abstract class BaseParseNs<T> extends CommonTask {

    /**
     * Event元数据库类
     */
    protected final MetaData<T> metadata;
    /**
     * 表过滤策略 正则表达式处理,着重处理ddl操作表
     */
    protected final String dbTableWhite;
    /**
     * mongoClient
     */
    protected final MongoClient mongoClient;
    /**
     * 每个ns队列最大缓存个数
     */
    protected final int maxQueueSizeOfNs;

    protected BaseParseNs(String workName, String dbTableWhite, String dsName, int maxQueueSizeOfNs) {
        super(workName, dsName);
        this.dbTableWhite = dbTableWhite;
        this.workName = workName;
        this.metadata = MetaData.getMetadata(workName);
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);
        this.maxQueueSizeOfNs = maxQueueSizeOfNs;
    }

    public void exe() {
        int count = 0;
        // 上次清除ns的时间
        long lastCleanNsTime = System.currentTimeMillis();
        while (true) {
            // 要加上异常处理 以防出现解析ns的线程异常退出
            T event = null;
            try {
                // 从原始的eventList进行解析，获取对应的ns
                event = metadata.getQueueOfEvent().poll();
                if (event != null) {
                    // 解析ns
                    parseNs(event);
                } else {
                    // 要是oplog太慢,count增加,lastCleanNsTime减少,此时也及时进行清除ns。
                    count += 1000;
                    lastCleanNsTime -= 1000;
                    // 代表changeStream队列为空 暂时休眠
                    TimeUnit.SECONDS.sleep(1);
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_STOP) {
                        break;
                    }
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                        // 发生了限速就开始限制读取
                        while (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                            TimeUnit.SECONDS.sleep(5);
                        }
                    }
                }
                // 每100w条 && 10分钟 清除一下空闲ns表信息
                if (count++ > 1000000) {
                    count = 0;
                    long currentTimeMillis = System.currentTimeMillis();
                    // 10分钟
                    if ((currentTimeMillis - lastCleanNsTime) > 1000 * 60 * 10) {
                        lastCleanNsTime = currentTimeMillis;
                        log.warn("{} start removing redundant ns buckets", workName);
                        for (Map.Entry<String, BlockingQueue<T>> queueEntry : metadata.getQueueOfNsMap().entrySet()) {
                            try {
                                BlockingQueue<T> value = queueEntry.getValue();
                                String key = queueEntry.getKey();
                                AtomicBoolean atomicBoolean = metadata.getStateOfNsMap().get(key);
                                boolean pre = atomicBoolean.get();
                                // cas操作
                                if (value.isEmpty() && !pre && atomicBoolean.compareAndSet(false, true)) {
                                    metadata.getQueueOfNsMap().remove(key);
                                    metadata.getStateOfNsMap().remove(key);
                                    atomicBoolean.set(false);
                                }
                            } catch (Exception e) {
                                log.error("{} error in clearing free ns queue of oplog,msg:{}", workName, e.getMessage());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (event != null) {
                    log.error("{} currently parsing the changeStream log:{}", workName, event.toString());
                }
                log.error("{} an error occurred in the split table thread of the changeStream,msg:{}", workName, e.getMessage());
            }
        }
    }

    public abstract void parseNs(T event) throws InterruptedException;


    public void pushQueue(String ns, T event, boolean isDDL) throws InterruptedException {
        // 多重DDL 保证DDL顺序性问题
        if (isDDL) {
            metadata.waitCacheExe();
        }
        if (!metadata.getQueueOfNsMap().containsKey(ns)) {
            metadata.getQueueOfNsMap().put(ns, new LinkedBlockingQueue<>(maxQueueSizeOfNs));
            metadata.getStateOfNsMap().put(ns, new AtomicBoolean());
            // 更新此表的唯一索引情况
            Document updateIndexInfo = new Document();
            updateIndexInfo.put("op", "updateIndexInfo");
            metadata.getQueueOfNsMap().get(ns).put(event);
            // 保证DDL顺序性问题
            metadata.waitCacheExe();
        }
        metadata.getQueueOfNsMap().get(ns).put(event);
        if (isDDL) {
            metadata.waitCacheExe();
        }
    }
}
