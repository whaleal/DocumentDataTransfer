package test;

import com.whaleal.ddt.thread.pool.ThreadPoolManager;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicInteger;

public class Reactive {

    public static void main(String[] args) {
        AtomicInteger counter = new AtomicInteger();

        Flux<Integer> infiniteProducer = Flux.generate(sink -> {
            int value = counter.incrementAndGet();
            System.out.println("source: " + value + ",threadName:" + Thread.currentThread().getName());
            sink.next(value);
        });

        new ThreadPoolManager("source", 10, 10, Integer.MAX_VALUE);
        new ThreadPoolManager("target", 10, 10, Integer.MAX_VALUE);

        Flux<Integer> sourceFlux = source(infiniteProducer);
        target(sourceFlux);
    }

    public static Flux<Integer> source(Flux<Integer> infiniteProducer) {
        return infiniteProducer.subscribeOn(Schedulers.fromExecutor(ThreadPoolManager.getPool("source").getExecutorService()));
    }

    public static void target(Flux<Integer> sourceFlux) {
        sourceFlux
                .publishOn(Schedulers.fromExecutor(ThreadPoolManager.getPool("target").getExecutorService()))
                .subscribe(item -> System.out.println("target: " + item + ",threadName:" + Thread.currentThread().getName()));
    }
}
