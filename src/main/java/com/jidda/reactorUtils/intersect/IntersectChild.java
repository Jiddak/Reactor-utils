package com.jidda.reactorUtils.intersect;

import com.jidda.reactorUtils.InnerConsumer;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class IntersectChild<T> implements InnerConsumer<T>, Disposable {

    final IntersectParent<T> parent;

    final int id;

    final long prefetch;

    volatile Subscription subscription;

    final static AtomicReferenceFieldUpdater<IntersectChild, Subscription>
            SUBSCRIPTION =
            AtomicReferenceFieldUpdater.newUpdater(IntersectChild.class,
                    Subscription.class,
                    "subscription");

    public IntersectChild(IntersectParent<T> parent, int id, long prefetch) {
        this.parent = parent;
        this.id = id;
        this.prefetch = prefetch;
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
    @Nullable
    public Object scanUnsafe(Scannable.Attr key) {
        if (key == Scannable.Attr.PARENT) return subscription;
        if (key == Scannable.Attr.ACTUAL ) return parent;
        if (key == Scannable.Attr.CANCELLED) return isDisposed();

        return null;
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
    public void onNext(T t) {
        parent.innerValue(id, t);
        subscription.request(1);
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
