package com.jidda.reactorUtils;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class JoinIfTest {

    @Test
    @SuppressWarnings("unchecked")
    public void AlwaysJoin(){

        Flux<String> f1 = Flux.just("A","B","C");
        Flux<Integer> f2 = Flux.just(1,2,3);

        StepVerifier.create(
                ReactorUtils.joinIf(f1,
                        f2,
                        Tuples::of,
                        (a,b) -> true)
        ).expectNext(
                Tuples.of("A",1),
                Tuples.of("B",1),
                Tuples.of("C",1),
                Tuples.of("A",2),
                Tuples.of("B",2),
                Tuples.of("C",2),
                Tuples.of("A",3),
                Tuples.of("B",3),
                Tuples.of("C",3)
        )
                .expectComplete()
                .verify();
    }

    @Test
    public void TupleOnCondition(){

        Flux<String> f1 = Flux.just("A","B","C");
        Flux<Integer> f2 = Flux.just(1,2,3);

        final String alphabet = "ABC";

        StepVerifier.create(
                ReactorUtils.joinIf(f1,
                        f2,
                        Tuples::of,
                        (a,b) -> b.equals(alphabet.indexOf(a)+1)
                ))
                .expectNext(
                        Tuples.of("A",1),
                        Tuples.of("B",2),
                        Tuples.of("C",3)
                )
                .expectComplete()
                .verify();
    }

    @Test
    public void SingleOnCondition(){

        Flux<String> f1 = Flux.just("A","B","C");
        Flux<Integer> f2 = Flux.just(1,2,3);

        final String alphabet = "ABC";

        StepVerifier.create(
                ReactorUtils.joinIf(f1,
                f2,
                (a,b) -> a,
                (a,b) -> b.equals(alphabet.indexOf(a)+1)
                ))
                .expectNext("A", "B", "C")
                .expectComplete()
                .verify();
    }

    @Test
    public void LargeConcurrentIntersect(){
        Flux<String> f1 = Flux.fromIterable(WordLists.wordList1()).publishOn(Schedulers.boundedElastic());
        Flux<String> f2 = Flux.fromIterable(WordLists.wordList2()).publishOn(Schedulers.boundedElastic());

        StepVerifier.create(
                ReactorUtils.joinIf(
                        f1,f2,
                        Tuples::of,
                        (a,b) -> true
                ))
                .expectNextCount(1000000)
                .expectComplete()
                .verify();
    }

    @Test
    public void JoinError(){

        Flux<String> f1 = Flux.just("A","B","C");
        Flux<String> f2 = Flux.just("D","C","A").concatWith(Flux.error(new RuntimeException()));
        StepVerifier.create(
                ReactorUtils.joinIf(
                        f1,f2,
                        String::concat,
                        String::equals
                ))
                .expectNext("CC","AA")
                .expectError()
                .verify();
    }

    @Test
    public void JoinEmpty(){
        Flux<String> f1 = Flux.just("A","B","C");
        Flux<String> f2 = Flux.empty();
        StepVerifier.create(
                ReactorUtils.joinIf(
                        f1,f2,
                        String::concat,
                        String::equals
                ))
                .expectComplete()
                .verify();
    }

    @Test
    public void JoinMono(){
        Flux<String> f1 = Flux.just("A","B","C");
        Mono<String> f2 = Mono.just("B");
        StepVerifier.create(
                ReactorUtils.joinIf(
                        f1,f2,
                        String::concat,
                        String::equals
                ))
                .expectNext("BB")
                .expectComplete()
                .verify();
    }

    @Test
    public void JoinMonoAMono(){
        Mono<String> f1 = Mono.just("B");
        Mono<String> f2 = Mono.just("B");
        StepVerifier.create(
                ReactorUtils.joinIf(
                        f1,f2,
                        String::concat,
                        String::equals
                ))
                .expectNext("BB")
                .expectComplete()
                .verify();
    }

    @Test
    public void JoinMonoAMonoNoMatch(){
        Mono<String> f1 = Mono.just("B");
        Mono<String> f2 = Mono.just("C");
        StepVerifier.create(
                ReactorUtils.joinIf(
                        f1,f2,
                        String::concat,
                        String::equals
                ))
                .expectComplete()
                .verify();
    }

    @Test
    public void JoinHotPublishers(){
        DirectProcessor<String> d1 = DirectProcessor.create();
        DirectProcessor<String> d2 = DirectProcessor.create();

        StepVerifier.create(
                ReactorUtils.joinIf(
                        d1,d2,
                        String::concat,
                        String::equals
                ))
                .expectSubscription()
                .then(() -> {
                    d1.onNext("A");
                    d2.onNext("B");
                    d1.onNext("C");
                    d1.onNext("B");
                })
                .expectNext("BB")
                .then(() -> d2.onNext("C"))
                .expectNext("CC")
                .then(() -> {
                    d1.onComplete();
                    d2.onComplete();
                })
                .expectComplete()
                .verify();
    }

    @Test
    public void JoinHotPublishers2(){
        DirectProcessor<String> d1 = DirectProcessor.create();
        DirectProcessor<String> d2 = DirectProcessor.create();

        StepVerifier.create(
                ReactorUtils.joinIf(
                        d1,d2,
                        String::concat,
                        String::equals
                ))
                .expectSubscription()
                .then(() -> {
                    d1.onNext("A");
                    d2.onNext("B");
                    d1.onNext("C");
                    d1.onNext("B");
                })
                .expectNext("BB")
                //Finish one publisher early
                //Then still push data on the other
                .then(() ->{
                    d1.onComplete();
                    d2.onNext("C");
                })
                .expectNext("CC")
                .then(d2::onComplete)
                .expectComplete()
                .verify();
    }


}
