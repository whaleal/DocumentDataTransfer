//package com.whaleal.ddt.reactive.read;
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
//
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
//    p void onSubscribe(Subscription subscription) {
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
//    }
//
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
