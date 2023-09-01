package test;


import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

public class MultipleProducers {
    public static void main(String[] args) {
        Flux<String> mergedProducer = Flux.concat();
        Runnable producer1 = () -> {
            mergedProducer.concatWith(Flux.just("1", "2", "3"));
        };
        new Thread(producer1).start();

        Runnable producer2 = () -> {
            mergedProducer.concatWith(Flux.just("4", "5", "6"));
        };
        new Thread(producer2).start();



        Runnable subscribe1 = () -> {
            while (true){
                System.out.println("====");
                mergedProducer.subscribe(System.out::println);
            }
        };
        new Thread(subscribe1).start();




        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}

