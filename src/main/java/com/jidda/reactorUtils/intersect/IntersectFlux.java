package com.jidda.reactorUtils.intersect;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

import java.util.Objects;

public class IntersectFlux<T> extends
        Flux<T> {

    final Flux<? extends T> leftSource;
    final Publisher<? extends T> rightSource;
    final long prefetch;

    public IntersectFlux(Flux<? extends T> leftSource,
                         Publisher<? extends T> rightSource,
                         long prefetch) {
        this.leftSource = Objects.requireNonNull(leftSource, "leftSource");
        this.rightSource = Objects.requireNonNull(rightSource, "rightSource");
        this.prefetch = prefetch;
    }


    public void subscribe(CoreSubscriber<? super T> actual) {
        IntersectSubscription<T> parent =
                new IntersectSubscription<>(actual,prefetch);

        actual.onSubscribe(parent);

        leftSource.subscribe(parent.getLeftSubscriber());
        rightSource.subscribe(parent.getRightSubscriber());
    }


}
