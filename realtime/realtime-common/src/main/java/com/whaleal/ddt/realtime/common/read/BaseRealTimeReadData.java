package com.whaleal.ddt.realtime.common.read;


import com.mongodb.client.MongoClient;
import com.whaleal.ddt.realtime.common.cache.MetaData;
import com.whaleal.ddt.sync.connection.MongoDBConnectionSync;
import com.whaleal.ddt.task.CommonTask;
import org.bson.BsonTimestamp;

/**
 * @author liheping
 */
public abstract class BaseRealTimeReadData<T> extends CommonTask {
    /**
     * mongoClient
     */
    protected final MongoClient mongoClient;
    /**
     * 表过滤策略 使用正则表达式进行过滤
     */
    protected final String dbTableWhite;
    /**
     * 开始读取该数据源的时间
     */
    protected int startTimeOfOplog;
    /**
     * 延迟时间s
     */
    protected int delayTime = 0;
    /**
     * 结束读取该oplog的时间
     */
    protected final int endTimeOfOplog;
    /**
     * 是否同步DDL
     */
    protected final boolean captureDDL;
    /**
     * event元数据库类保存数据信息的地方
     */
    protected final MetaData<T> metadata;
    /**
     * 是否读取完成
     */
    protected boolean isReadScanOver = false;
    /**
     * 数据源版本
     */
    protected final String dbVersion;

    /**
     * 最新oplog时间戳信息
     */
    protected BsonTimestamp lastOplogTs = new BsonTimestamp(0);

    protected BaseRealTimeReadData(String workName, String dsName, boolean captureDDL, String dbTableWhite, int startTimeOfOplog, int endTimeOfOplog, int delayTime) {
        super(workName, dsName);
        this.captureDDL = captureDDL;
        this.dbTableWhite = dbTableWhite;
        this.endTimeOfOplog = endTimeOfOplog;
        this.startTimeOfOplog = startTimeOfOplog;
        this.workName = workName;
        this.delayTime = delayTime;
        this.metadata = MetaData.getMetadata(workName);
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);
        this.dbVersion = MongoDBConnectionSync.getVersion(dsName);
    }

    public abstract void source();
}
