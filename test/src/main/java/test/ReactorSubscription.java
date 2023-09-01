package test;

/**
 * @projectName: full-common
 * @package: test
 * @className: ReactorSubscription
 * @author: Eric
 * @description: TODO
 * @date: 31/08/2023 13:42
 * @version: 1.0
 */
import reactor.core.publisher.Flux;

public class ReactorSubscription {
    public static void main(String[] args) {
        Flux<String> colors = Flux.just("Red", "Green", "Blue");

        colors.concatWith(Flux.just("12", "123"));



        colors.subscribe(
                color -> System.out.println("Received: " + color),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("Completed")
        );



        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
