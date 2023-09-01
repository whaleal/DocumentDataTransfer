package test;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicInteger;

public class InfiniteProducerConsumer {
    public static void main(String[] args) throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();

        Flux<Integer> infiniteProducer = Flux.generate(sink -> {
            int value = counter.incrementAndGet();
            System.out.println("Producing: " + value + " on thread: " + Thread.currentThread().getName());
            sink.next(value);
        });

        infiniteProducer
                .subscribeOn(Schedulers.newParallel("producer", 1)) // 生产者在单独线程中执行
                .publishOn(Schedulers.newParallel("consumer1", 4)) // 消费者1在并行线程中执行
                .subscribe(item -> {
                    System.out.println("Consumer 1 - Received: " + item + " on thread: " + Thread.currentThread().getName());
                });

        infiniteProducer
                .subscribeOn(Schedulers.newParallel("producer", 5)) // 生产者在单独线程中执行
                .publishOn(Schedulers.newParallel("consumer2", 4)) // 消费者2在并行线程中执行
                .subscribe(item -> {
                    System.out.println("Consumer 2 - Received: " + item + " on thread: " + Thread.currentThread().getName());
                });

        // 让主线程等待一段时间，模拟持续的数据生成和消费
        Thread.sleep(5000);
    }
}
