package com.jidda.reactorUtils;

import com.jidda.reactorUtils.intersect.IntersectFlux;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

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


    static abstract class UtilityFlux<T> extends Flux<T> {

        protected static <T> Flux<T> onAssembly(Flux<T> source) {
            return Flux.onAssembly(source);
        }
    }


}
