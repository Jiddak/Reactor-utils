package com.jidda.reactorUtils;

import com.jidda.reactorUtils.intersect.IntersectFlux;
import com.jidda.reactorUtils.join.ConditionalJoinFlux;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

public class ReactorUtils {

    public static final <T> Flux<T> intersect(
            Flux<? extends T> left,
            Publisher<? extends T> right,
            long prefetch
    ) {
        return UtilityFlux.onAssembly(new IntersectFlux<>(left,right,prefetch));
    }

    public static final <T> Flux<T> intersect(
            Flux<? extends T> left,
            Publisher<? extends T> right
    ) {
        return UtilityFlux.onAssembly(new IntersectFlux<>(left,right,Long.MAX_VALUE));
    }


    public static final <T> Flux<T> intersect(
            Publisher<? extends T> publisher1,
            Publisher<? extends T> publisher2,
            Publisher<? extends T> publisher3
            ) {
        return UtilityFlux.onAssembly(new IntersectFlux<>(Arrays.asList(publisher1,publisher2,publisher3),Long.MAX_VALUE));
    }


    public static final <T> Flux<T> intersect(
            List<? extends Publisher<? extends T>> publishers
    ) {
        return UtilityFlux.onAssembly(new IntersectFlux<>(publishers,Long.MAX_VALUE));
    }


    /**
     * Map values from two Publishers into time windows and emit combination of values
     * in case their windows overlap. The emitted elements are obtained by passing the
     * values from two {@link Publisher}s to a {@link BiPredicate} to determine if emission should occur
     * and then passing to a {@link BiFunction} to provide resultant value.
     * <p>
     * There are no guarantees in what order the items get combined when multiple items from
     * one or both source Publishers overlap.
     * @param leftSource the left {@link Publisher} to correlate items with
     * @param rightSource the right {@link Publisher} to correlate items with
     * @param resultSelector a {@link BiFunction} that takes items emitted by each {@link Publisher}
     * and returns the value to be emitted by the resulting {@link Flux}
     * @param condition {@link BiPredicate} that compares left and right element. If false no emission occurs
     * The collection is non blocking and can be accessed concurrently
     * As such, no guarantee can be made that the number of element returned will be exactly cacheSize
     * @param <TLeft> the type of the elements from the left {@link Publisher}
     * @param <TRight> the type of the elements from the right {@link Publisher}
     * @param <R> the combined result type
     *
     * @return a joining {@link Flux}
     */
    public static final <TLeft,TRight,R> Flux<R> joinIf(
            Publisher<TLeft> leftSource,
            Publisher<? extends TRight> rightSource,
            BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector,
            BiPredicate<TLeft,TRight> condition
    ) {
        return joinIf(leftSource,rightSource,resultSelector,condition,0L);
    }


    /**
     * Map values from two Publishers into time windows and emit combination of values
     * in case their windows overlap. The emitted elements are obtained by passing the
     * values from two {@link Publisher}s to a {@link BiPredicate} to determine if emission should occur
     * and then passing to a {@link BiFunction} to provide resultant value.
     * <p>
     * There are no guarantees in what order the items get combined when multiple items from
     * one or both source Publishers overlap.
     * @param leftSource the left {@link Publisher} to correlate items with
     * @param rightSource the right {@link Publisher} to correlate items with
     * @param resultSelector a {@link BiFunction} that takes items emitted by each {@link Publisher}
     * and returns the value to be emitted by the resulting {@link Flux}
     * @param condition {@link BiPredicate} that compares left and right element. If false no emission occurs
     * @param cacheSize The number of elements stored for comparison, < 1 is unbounded.
     * The collection is non blocking and can be accessed concurrently
     * As such, no guarantee can be made that the number of element returned will be exactly cacheSize
     * @param <TLeft> the type of the elements from the left {@link Publisher}
     * @param <TRight> the type of the elements from the right {@link Publisher}
     * @param <R> the combined result type
     *
     * @return a joining {@link Flux}
     */
    public static final <TLeft,TRight,R> Flux<R> joinIf(
            Publisher<TLeft> leftSource,
            Publisher<? extends TRight> rightSource,
            BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector,
            BiPredicate<TLeft,TRight> condition,
            long cacheSize
    ) {
        return UtilityFlux.onAssembly(
                new ConditionalJoinFlux<>(leftSource,
                        rightSource,
                        s -> Mono.never(),
                        s -> Mono.never(),
                        resultSelector,
                        condition,
                        Long.MAX_VALUE,
                        cacheSize
                ));
    }


    static abstract class UtilityFlux<T> extends Flux<T> {

        protected static <T> Flux<T> onAssembly(Flux<T> source) {
            return Flux.onAssembly(source);
        }
    }


}
