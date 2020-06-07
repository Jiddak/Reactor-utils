package com.jidda.reactorUtils;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

//Copied from reactor core
public interface InnerProducer<O>
        extends Scannable, Subscription {

    CoreSubscriber<? super O> actual();

    @Override
    @Nullable
    default Object scanUnsafe(Attr key){
        if (key == Attr.ACTUAL) {
            return actual();
        }
        return null;
    }

}
