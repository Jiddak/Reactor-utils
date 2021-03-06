package com.jidda.reactorUtils;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

public class IntersectTest {


    @Test
    public void BasicIntersect(){

        Flux<String> f1 = Flux.just("A","B","C");
        Flux<String> f2 = Flux.just("D","C","A");
        StepVerifier.create(ReactorUtils.intersect(f1,f2))
                .expectNext("C","A")
        .expectComplete()
        .verify();
    }

    @Test
    public void IntersectError(){

        Flux<String> f1 = Flux.just("A","B","C");
        Flux<String> f2 = Flux.just("D","C","A").concatWith(Flux.error(new RuntimeException()));
        StepVerifier.create(ReactorUtils.intersect(f1,f2))
                .expectNext("C","A")
                .expectError()
                .verify();
    }


    @Test
    public void IntersectEmpty(){
        Flux<String> f1 = Flux.just("A","B","C");
        Flux<String> f2 = Flux.empty();
        StepVerifier.create(ReactorUtils.intersect(f1,f2))
                .expectComplete()
                .verify();
    }

    @Test
    public void IntersectMono(){
        Flux<String> f1 = Flux.just("A","B","C");
        Mono<String> f2 = Mono.just("B");
        StepVerifier.create(ReactorUtils.intersect(f1,f2))
                .expectNext("B")
                .expectComplete()
                .verify();
    }


    @Test
    public void ListOfPublishers(){
        List<Flux<String>> fluxes = Arrays.asList(
                Flux.just("A","B","C"),
                Flux.just("D","C","A"),
                Flux.just("F","B","D")
        );
        StepVerifier.create(ReactorUtils.intersect(fluxes))
                .expectNext("C","A","B","D")
                .expectComplete()
                .verify();
    }

    @Test
    public void LargeConcurrentIntersect(){
        Flux<String> f1 = Flux.fromIterable(WordLists.wordList1()).publishOn(Schedulers.boundedElastic());
        Flux<String> f2 = Flux.fromIterable(WordLists.wordList2()).publishOn(Schedulers.boundedElastic());

        StepVerifier.create(ReactorUtils.intersect(f1,f2))
                .expectNextCount(394)
                .expectComplete()
                .verify();


    }

    @Test
    public void IntersectHotPublishers(){
        DirectProcessor<String> d1 = DirectProcessor.create();
        DirectProcessor<String> d2 = DirectProcessor.create();

        StepVerifier.create(ReactorUtils.intersect(d1,d2))
                .expectSubscription()
                .then(() -> {
                    d1.onNext("A");
                    d2.onNext("B");
                    d1.onNext("C");
                    d1.onNext("B");
                })
                .expectNext("B")
                .then(() -> d2.onNext("C"))
                .expectNext("C")
                .then(() -> {
                    d1.onComplete();
                    d2.onComplete();
                })
                .expectComplete()
                .verify();
    }

    @Test
    public void IntersectHotPublishers2(){
        DirectProcessor<String> d1 = DirectProcessor.create();
        DirectProcessor<String> d2 = DirectProcessor.create();

        StepVerifier.create(ReactorUtils.intersect(d1,d2))
                .expectSubscription()
                .then(() -> {
                    d1.onNext("A");
                    d2.onNext("B");
                    d1.onNext("C");
                    d1.onNext("B");
                })
                .expectNext("B")
                //Finish one publisher early
                //Then still push data on the other
                .then(() ->{
                    d1.onComplete();
                    d2.onNext("C");
                })
                .expectNext("C")
                .then(d2::onComplete)
                .expectComplete()
                .verify();
    }

    @Test
    public void ListOfHotPublishers(){
        DirectProcessor<String> d1 = DirectProcessor.create();
        DirectProcessor<String> d2 = DirectProcessor.create();
        DirectProcessor<String> d3 = DirectProcessor.create();

        List<Flux<String>> fluxes = Arrays.asList(d1,d2,d3);

        StepVerifier.create(ReactorUtils.intersect(fluxes))
                .expectSubscription()
                .then(() -> {
                    d1.onNext("A");
                    d2.onNext("D");
                    d2.onNext("C");
                    d1.onNext("B");
                    d1.onNext("C");
                    d3.onNext("F");
                })
                .expectNext("C")
                //Finish one publisher early
                //Then still push data on the other
                .then(() ->{
                    d1.onComplete();
                    d3.onNext("B");
                })
                .expectNext("B")
                .then(() ->{
                    d2.onComplete();
                    d3.onNext("D");
                })
                .expectNext("D")
                .then(d3::onComplete)
                .expectComplete()
                .verify();
    }


}
