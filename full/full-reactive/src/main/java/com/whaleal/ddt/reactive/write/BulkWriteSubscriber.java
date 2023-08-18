package com.whaleal.ddt.reactive.write;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.WriteModel;
import com.whaleal.ddt.common.cache.FullMetaData;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.List;

@Log4j2
public class BulkWriteSubscriber implements Subscriber<BulkWriteResult> {

    private Subscription subscription;

    private FullMetaData fullMetaData;

    private List<WriteModel<Document>> writeModelList;

    private String ns;

    private String workName;

    public BulkWriteSubscriber(FullMetaData fullMetaData, List<WriteModel<Document>> writeModelList, String ns, String workName) {
        this.fullMetaData = fullMetaData;
        this.writeModelList = writeModelList;
        this.ns = ns;
        this.workName = workName;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(BulkWriteResult bulkWriteResult) {
        int insertedCount = bulkWriteResult.getInsertedCount();
        fullMetaData.getWriteDocCount().add(insertedCount);
    }

    @Override
    public void onError(Throwable t) {
        // todo 查看其他信息 例如异常信息 和主键重复的信息
        // 当发生异常 把数据重新放回队列 重新写入
        log.error("{} bulkWriteSubscriber exception occurs :{}", workName, t.getMessage());
    }

    @Override
    public void onComplete() {
        // 可以不打印信息
        writeModelList = null;
    }
}
