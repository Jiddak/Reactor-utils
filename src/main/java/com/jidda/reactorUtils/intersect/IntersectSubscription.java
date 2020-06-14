package com.jidda.reactorUtils.intersect;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.*;
import reactor.core.publisher.Operators;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

class IntersectSubscription<T> implements IntersectParent<T> {

    public final Disposable.Composite cancellations;

    final Queue<Tuple2<Integer,T>> queue;

    final Set<T> intersects;

    final List<IntersectChild<T>> subscribers;

    final CoreSubscriber<? super T> actual;

    final List<? extends Publisher<? extends T>> sources;

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

    public IntersectSubscription(CoreSubscriber<? super T> actual,List<? extends Publisher<? extends T>> sources) {
        this.actual = actual;
        this.cancellations = Disposables.composite();
        this.queue = new ConcurrentLinkedQueue<>();
        this.intersects = new LinkedHashSet<>();
        this.subscribers = Collections.synchronizedList(new ArrayList<>());
        this.sources = sources;
        ACTIVE.lazySet(this,sources.size());
    }

    public void subscribeToChildren(long prefetch){
        for(Publisher<? extends T> source : sources){
            source.subscribe(getNewSubscriber(prefetch));
        }
    }

    private IntersectChild<T> getNewSubscriber(long prefetch) {
        int id = subscribers.size();
        IntersectChild<T> subscriber = new IntersectChild<>(this, id,prefetch);
        cancellations.add(subscriber);
        subscribers.add(subscriber);
        return subscriber;
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

        subscribers.forEach(sub -> sub.getPrevious().clear());

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

                    subscribers.forEach(sub -> sub.getPrevious().clear());
                    cancellations.dispose();

                    subscriber.onComplete();
                    return;
                }

                if (empty) {
                    break;
                }

                process(mode,subscriber);
            }
            missed = WIP.addAndGet(this, -missed);
        } while (missed != 0);
    }

    private void process(Tuple2<Integer, T> mode, Subscriber<? super T> subscriber){
        T value = mode.getT2();
        Integer subscriberId = mode.getT1();
        IntersectChild<T> subscribingChild = subscribers.get(subscriberId);

        if(! intersects.contains(value)){
            for(IntersectChild<T> child : subscribers) {
                if(child.equals(subscribingChild))
                    continue;
                if(child.getPrevious().contains(value)) {
                    subscribingChild.getPrevious().remove(value);
                    intersects.add(value);
                }
            }
        }
        if(intersects.contains(value))
            subscriber.onNext(value);
        else
            subscribingChild.getPrevious().add(value);
    }

/*    private void process(T value,Subscriber<? super T> subscriber){
        if(! intersects.contains(value) && rightSubscriber.getPrevious().contains(value)){
            rightSubscriber.getPrevious().remove(value);
            intersects.add(value);
            subscriber.onNext(value);
        }
        else
            leftSubscriber.getPrevious().add(value);
    }

    public void processRight(T value,Subscriber<? super T> subscriber){
        if(! intersects.contains(value) && leftSubscriber.getPrevious().contains(value)){
            leftSubscriber.getPrevious().remove(value);
            intersects.add(value);
            subscriber.onNext(value);
        }
        else
            leftSubscriber.getPrevious().add(value);

    }*/

}
