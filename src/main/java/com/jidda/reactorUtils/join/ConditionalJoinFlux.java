package com.jidda.reactorUtils.join;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

public class ConditionalJoinFlux<TRight,TLeft,TLeftEnd,TRightEnd,R>
        extends Flux<R>{

    final Publisher<? extends TLeft> leftSource;

    final Publisher<? extends TRight> rightSource;

    final Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd;

    final Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd;

    final BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector;

    final BiPredicate<TLeft,TRight> condition;

    final Long prefetch;

    public ConditionalJoinFlux(Publisher<TLeft> leftSource,
                               Publisher<? extends TRight> rightSource,
                               Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd,
                               Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd,
                               BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector,
                               BiPredicate<TLeft, TRight> condition,
                               Long prefetch) {
        this.leftSource = Objects.requireNonNull(leftSource, "leftSource");
        this.rightSource = Objects.requireNonNull(rightSource, "rightSource");
        this.leftEnd = Objects.requireNonNull(leftEnd, "leftEnd");
        this.rightEnd = Objects.requireNonNull(rightEnd, "rightEnd");
        this.resultSelector = Objects.requireNonNull(resultSelector, "resultSelector");
        this.condition = Objects.requireNonNull(condition, "condition");
        this.prefetch = prefetch;
    }

    public void subscribe(CoreSubscriber<? super R> actual) {
        ConditionalJoinSubscription<TLeft, TRight, TLeftEnd, TRightEnd, R> parent =
                new ConditionalJoinSubscription<>(actual,leftEnd,rightEnd,resultSelector,condition,prefetch);

        actual.onSubscribe(parent);

        leftSource.subscribe(parent.getLeftSubscriber());
        rightSource.subscribe(parent.getRightSubscriber());
    }

}
