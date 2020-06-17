package com.jidda.reactorUtils;

import com.jidda.reactorUtils.intersect.IntersectFlux;
import com.jidda.reactorUtils.join.ConditionalJoinFlux;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

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


    public static final <TLeft,TRight, TLeftEnd, TRightEnd, R> Flux<R> joinIf(
            Publisher<TLeft> leftSource,
            Publisher<? extends TRight> rightSource,
            BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector,
            BiPredicate<TLeft,TRight> condition
    ) {
        return UtilityFlux.onAssembly(
                new ConditionalJoinFlux<>(leftSource,
                        rightSource,
                        s -> Mono.empty(),
                        s -> Mono.empty(),
                        resultSelector,
                        condition,
                        Long.MAX_VALUE
                ));
    }


    static abstract class UtilityFlux<T> extends Flux<T> {

        protected static <T> Flux<T> onAssembly(Flux<T> source) {
            return Flux.onAssembly(source);
        }
    }


}
