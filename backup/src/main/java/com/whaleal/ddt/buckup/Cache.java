package com.whaleal.ddt.buckup;


import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author liheping
 */
@Getter
public class Cache<T> {

    private BlockingQueue<T> queueOfEvent;

    public Cache(int queueOfEventSize) {
        queueOfEvent = new LinkedBlockingQueue<>(queueOfEventSize);
    }

    private volatile long oplogTs;
    @Setter
    private volatile String wapURL = "";

    public void setOplogTs(long oplogTs) {
        this.oplogTs = oplogTs;
    }
}
