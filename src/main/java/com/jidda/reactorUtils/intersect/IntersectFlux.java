package com.jidda.reactorUtils.intersect;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class IntersectFlux<T> extends
        Flux<T> {

    final List<? extends Publisher<? extends T>> sources;
    final long prefetch;

    public IntersectFlux(Publisher<? extends T> leftSource,
                         Publisher<? extends T> rightSource,
                         long prefetch) {
        this(Arrays.asList(
                Objects.requireNonNull(leftSource, "leftSource"),
                Objects.requireNonNull(rightSource, "rightSource")
        ),prefetch);
    }

    public IntersectFlux(List<? extends Publisher<? extends T>> sources,long prefetch) {
        this.sources = sources;
        this.prefetch = prefetch;
    }


    public void subscribe(CoreSubscriber<? super T> actual) {
        IntersectSubscription<T> parent =
                new IntersectSubscription<>(actual,sources);
        actual.onSubscribe(parent);
        parent.subscribeToChildren(prefetch);

    }


}
