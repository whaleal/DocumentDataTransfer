package test;


import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * @author liheping
 */
class MultiProducerExample {
    public static void main(String[] args) {
        Flux<Integer> producer1 = Flux.range(1, 5);
        Flux<Integer> producer2 = Flux.range(6, 5);

        Flux<Integer> mergedProducer = Flux.merge(producer1, producer2);

        mergedProducer.publishOn(Schedulers.newParallel("consumer1", 4))
                .subscribe(item -> {
                    System.out.println("Consumer 1 - Received: " + item + " on thread: " + Thread.currentThread().getName());
                });

        mergedProducer.publishOn(Schedulers.newParallel("consumer2", 4))
                .subscribe(item -> {
                    System.out.println("Consumer 2 - Received: " + item + " on thread: " + Thread.currentThread().getName());
                });
    }
}

