package test;


import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactorExample {
    public static void main(String[] args) {
        Flux<String> flux = Flux.just("Hello", "World");
        System.out.println(flux);
        Mono<String> mono = Mono.just("Hello");
    }
}

