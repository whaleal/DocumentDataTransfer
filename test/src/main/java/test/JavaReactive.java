package test;

import com.whaleal.ddt.thread.pool.ThreadPoolManager;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @projectName: full-common
 * @package: test
 * @className: JavaReactive
 * @author: Eric
 * @description: TODO
 * @date: 01/09/2023 16:39
 * @version: 1.0
 */
public class JavaReactive {

    static Flux<Long> flux =  Flux.create((Consumer<FluxSink<Long>>) fluxSink -> fluxSink.next(System.currentTimeMillis()));

    static {
        new ThreadPoolManager("source", 10, 10, Integer.MAX_VALUE);
        new ThreadPoolManager("target", 10, 10, Integer.MAX_VALUE);


        flux.publishOn(Schedulers.fromExecutor(ThreadPoolManager.getPool("source").getExecutorService()));
        flux.subscribeOn(Schedulers.fromExecutor(ThreadPoolManager.getPool("target").getExecutorService()));
    }

    public static void source() {
        // 生成者
        flux.publish(longFlux -> (Publisher<Long>) s -> s.onNext(System.currentTimeMillis()));

    }

    public static void main(String[] args) {
        List<Long> longList = new ArrayList<>();

        source();
        target();

        while (true) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void target() {
        final Subscriber<Long> subscriber = new Subscriber<Long>() {
            Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                this.subscription.request(1);
            }

            @Override
            public void onNext(Long aLong) {
                System.out.println(aLong);
                this.subscription.request(1);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {

            }
        };

        flux.subscribe(subscriber);


    }
}
