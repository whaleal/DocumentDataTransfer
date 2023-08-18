//package com.whaleal.ddt.sync.task.read;
//
//import com.mongodb.client.model.InsertOneModel;
//import com.mongodb.client.model.WriteModel;
//import com.whaleal.ddt.cache.BatchDataEntity;
//import com.whaleal.ddt.common.cache.FullMetaData;
//import lombok.extern.log4j.Log4j2;
//import org.bson.Document;
//import org.reactivestreams.Subscriber;
//import org.reactivestreams.Subscription;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * @projectName: full-common
// * @package: com.whaleal.ddt.sync.task.read
// * @className: ReadSubscriber
// * @author: Eric
// * @description: TODO
// * @date: 18/08/2023 11:42
// * @version: 1.0
// */
//@Log4j2
//public class ReadSubscriber implements Subscriber<Document> {
//
//    private int batchSize = 128;
//
//    private int totalReadNum = 0;
//
//    private List<WriteModel<Document>> dataList = new ArrayList<>();
//
//    private Subscription subscription;
//
//    private FullMetaData fullMetaData;
//
//    private
//
//    @Override
//    public void onSubscribe(Subscription subscription) {
//        this.subscription = subscription;
//        subscription.request(batchSize);
//    }
//
//    @Override
//    public void onNext(Document document) {
//        dataList.add(new InsertOneModel<>(document));
//        if (totalReadNum++ % batchSize == 0) {
//            subscription.request(batchSize);
//            dataList = new ArrayList<>();
//            fullMetaData.putData();
//        }
//
//    }
////
////    public void putDataToCache() {
////        BatchDataEntity batchDataEntity = new BatchDataEntity();
////        batchDataEntity.setDataList(this.dataList);
////        batchDataEntity.setNs(this.taskMetadata.getNs());
////        batchDataEntity.setSourceDsName(this.taskMetadata.getSourceDsName());
////        batchDataEntity.setBatchNo(System.currentTimeMillis());
////        // 推送数据到缓存区中
////        this.fullMetaData.putData(batchDataEntity);
////        // 设置读取条数
////        this.fullMetaData.getReadDocCount().add(this.dataList.size());
////        this.dataList = new ArrayList<>();
////        this.cacheTemp = 0;
////    }
//
//    @Override
//    public void onError(Throwable t) {
//
//    }
//
//    @Override
//    public void onComplete() {
//
//    }
//}
