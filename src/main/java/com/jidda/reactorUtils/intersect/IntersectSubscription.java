package com.jidda.reactorUtils.intersect;
import org.reactivestreams.Subscriber;
import reactor.core.*;
import reactor.core.publisher.Operators;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.LinkedHashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

public class IntersectSubscription<T> implements IntersectParent<T> {

    public final Disposable.Composite cancellations;

    final Queue<Tuple2<Integer,T>> queue;

    final Set<T> lefts;

    final Set<T> rights;

    final Set<T> intersects;

    final IntersectChild<T> leftSubscriber;

    final IntersectChild<T> rightSubscriber;

    final CoreSubscriber<? super T> actual;

    volatile int wip;

    static final AtomicIntegerFieldUpdater<IntersectSubscription> WIP =
            AtomicIntegerFieldUpdater.newUpdater(IntersectSubscription.class, "wip");

    volatile int active;

    static final AtomicIntegerFieldUpdater<IntersectSubscription> ACTIVE =
            AtomicIntegerFieldUpdater.newUpdater(IntersectSubscription.class,
                    "active");

    volatile long requested;

    static final AtomicLongFieldUpdater<IntersectSubscription> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(IntersectSubscription.class,
                    "requested");

    volatile Throwable error;

    static final AtomicReferenceFieldUpdater<IntersectSubscription, Throwable> ERROR =
            AtomicReferenceFieldUpdater.newUpdater(IntersectSubscription.class,
                    Throwable.class,
                    "error");

    public IntersectSubscription(CoreSubscriber<? super T> actual,long prefetch) {
        this.actual = actual;
        this.cancellations = Disposables.composite();
        this.queue = new ConcurrentLinkedQueue<>();
        this.lefts = new LinkedHashSet<>();
        this.rights = new LinkedHashSet<>();
        this.intersects = new LinkedHashSet<>();
        ACTIVE.lazySet(this, 2);
        leftSubscriber = new IntersectChild<>(this, 0,prefetch);
        cancellations.add(leftSubscriber);
        rightSubscriber = new IntersectChild<>(this, 1,prefetch);
        cancellations.add(rightSubscriber);
    }

    public IntersectChild<T> getRightSubscriber() {
        return rightSubscriber;
    }

    public IntersectChild<T> getLeftSubscriber(){
        return leftSubscriber;
    }

    @Override
    public final CoreSubscriber<? super T> actual() {
        return actual;
    }

    @Override
    public Stream<? extends Scannable> inners() {
        return Scannable.from(cancellations).inners();
    }

    @Override
    public void request(long n) {
        if (Operators.validate(n)) {
            Operators.addCap(REQUESTED, this, n);
        }
    }

    @Override
    public void cancel() {
        if (cancellations.isDisposed()) {
            return;
        }
        cancellations.dispose();
        if (WIP.getAndIncrement(this) == 0) {
            queue.clear();
        }
    }

    void errorAll(Subscriber<?> subscriber) {
        Throwable ex = Exceptions.terminate(ERROR, this);

        lefts.clear();
        rights.clear();

        subscriber.onError(ex);
    }

    @Override
    public void innerError(Throwable ex) {
        if (Exceptions.addThrowable(ERROR, this, ex)) {
            ACTIVE.decrementAndGet(this);
            drain();
        }
        else {
            Operators.onErrorDropped(ex, actual.currentContext());
        }
    }

    @Override
    public void innerComplete(IntersectChild<T> child) {
        cancellations.remove(child);
        ACTIVE.decrementAndGet(this);
        drain();
    }

    @Override
    public void innerValue(Integer id, T o) {
        queue.add(Tuples.of(id,o));
        drain();
    }

    void drain() {
        if (WIP.getAndIncrement(this) != 0) {
            return;
        }

        int missed = 1;
        Queue<Tuple2<Integer,T>> q = queue;
        Subscriber<? super T> subscriber = actual;

        do {
            for (; ; ) {
                if (cancellations.isDisposed()) {
                    q.clear();
                    return;
                }

                Throwable ex = error;
                if (ex != null) {
                    q.clear();
                    cancellations.dispose();
                    errorAll(subscriber);
                    return;
                }

                boolean isActive = active == 0;

                Tuple2<Integer, T> mode = q.poll();

                boolean empty = mode == null;

                if (isActive && empty) {

                    lefts.clear();
                    rights.clear();
                    cancellations.dispose();

                    subscriber.onComplete();
                    return;
                }

                if (empty) {
                    break;
                }

                if (mode.getT1().equals(0)) {
                    processLeft(mode.getT2(), subscriber);
                } else if (mode.getT1().equals(1)) {
                    processRight(mode.getT2(), subscriber);
                }
            }
            missed = WIP.addAndGet(this, -missed);
        } while (missed != 0);
    }

    private void processLeft(T value,Subscriber<? super T> subscriber){
        if(! intersects.contains(value) && rights.contains(value)){
            rights.remove(value);
            intersects.add(value);
            subscriber.onNext(value);
        }
        else
            lefts.add(value);
    }

    public void processRight(T value,Subscriber<? super T> subscriber){
        if(! intersects.contains(value) && lefts.contains(value)){
            lefts.remove(value);
            intersects.add(value);
            subscriber.onNext(value);
        }
        else
            rights.add(value);

    }

}
