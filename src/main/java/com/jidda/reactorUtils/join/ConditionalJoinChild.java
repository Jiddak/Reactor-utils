package com.jidda.reactorUtils.join;

import com.jidda.reactorUtils.InnerConsumer;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

class ConditionalJoinChild<T,R> implements InnerConsumer<T>, Disposable {

    final private ConditionalJoinParent<R> parent;

    final private List<T> previous;

    final private int nextId;

    final private int closeId;

    final private long prefetch;

    volatile Subscription subscription;

    final static AtomicReferenceFieldUpdater<ConditionalJoinChild, Subscription>
            SUBSCRIPTION =
            AtomicReferenceFieldUpdater.newUpdater(ConditionalJoinChild.class,
                    Subscription.class,
                    "subscription");

    public ConditionalJoinChild(ConditionalJoinParent<R> parent, int nextId, int closeId, long prefetch) {
        this.parent = parent;
        this.nextId = nextId;
        this.closeId = closeId;
        this.prefetch = prefetch;
        this.previous = new  LinkedList<>();
    }

    public List<T> getPrevious() {
        return previous;
    }

    @Override
    public void dispose() {
        Operators.terminate(SUBSCRIPTION, this);
    }

    @Override
    public Context currentContext() {
        return parent.actual().currentContext();
    }


    @Override
    public boolean isDisposed() {
        return Operators.cancelledSubscription() == subscription;
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (Operators.setOnce(SUBSCRIPTION, this, s)) {
            s.request(prefetch);
        }
    }

    @Override
    public void onNext(Object t) {
        parent.innerValue(nextId, t);
        subscription.request(1);
    }

    @Override
    @Nullable
    public Object scanUnsafe(Scannable.Attr key) {
        if (key == Scannable.Attr.PARENT) return subscription;
        if (key == Scannable.Attr.ACTUAL ) return parent;
        if (key == Scannable.Attr.CANCELLED) return isDisposed();

        return null;
    }

    @Override
    public void onError(Throwable t) {
        parent.innerError(t);
    }

    @Override
    public void onComplete() {
        parent.innerComplete(this);
    }


}
