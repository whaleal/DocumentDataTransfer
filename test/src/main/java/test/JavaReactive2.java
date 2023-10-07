package test;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * @projectName: full-common
 * @package: test
 * @className: JavaReactive
 * @author: Eric
 * @description: TODO
 * @date: 01/09/2023 16:39
 * @version: 1.0
 */
public class JavaReactive2 {
    public static void main(String[] args) {

        Publisher<Long> publisher = new Publisher<Long>() {
            @Override
            public void subscribe(Subscriber<? super Long> s) {
                s.onNext(Math.round(Math.random()));
            }
        };

//        publisher.doOnNext(event -> System.out.println("receive event: " + event)).subscribe();
//
//        publisher.onNext(1); // print 'receive event: 1'
//        publisher.onNext(2); // print 'receive event: 2'
    }
}
