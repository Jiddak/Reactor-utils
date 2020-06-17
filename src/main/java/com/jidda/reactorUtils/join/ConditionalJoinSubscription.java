package com.jidda.reactorUtils.join;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.*;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;

import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

class ConditionalJoinSubscription<TLeft, TRight, TLeftEnd, TRightEnd, R> implements ConditionalJoinParent<R> {

    public final Disposable.Composite cancellations;

    final Queue<Object> queue;
    final BiPredicate<Object, Object> queueBiOffer;

    final ConditionalJoinChild<TLeft, R> leftSubscriber;

    final ConditionalJoinChild<TRight, R> rightSubscriber;

    final BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector;

    final BiPredicate<TLeft, TRight> condition;

    final CoreSubscriber<? super R> actual;

    volatile int wip;

    static final AtomicIntegerFieldUpdater<ConditionalJoinSubscription> WIP =
            AtomicIntegerFieldUpdater.newUpdater(ConditionalJoinSubscription.class, "wip");

    volatile int active;

    static final AtomicIntegerFieldUpdater<ConditionalJoinSubscription> ACTIVE =
            AtomicIntegerFieldUpdater.newUpdater(ConditionalJoinSubscription.class,
                    "active");

    volatile long requested;

    static final AtomicLongFieldUpdater<ConditionalJoinSubscription> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(ConditionalJoinSubscription.class,
                    "requested");

    volatile Throwable error;

    static final AtomicReferenceFieldUpdater<ConditionalJoinSubscription, Throwable> ERROR =
            AtomicReferenceFieldUpdater.newUpdater(ConditionalJoinSubscription.class,
                    Throwable.class,
                    "error");

    static final Integer LEFT_VALUE = 1;

    static final Integer RIGHT_VALUE = 2;

    static final Integer LEFT_CLOSE = 3;

    static final Integer RIGHT_CLOSE = 4;

    @SuppressWarnings("unchecked")
    public ConditionalJoinSubscription(CoreSubscriber<? super R> actual,
                                       Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd,
                                       Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd,
                                       BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector,
                                       BiPredicate<TLeft, TRight> condition,
                                       Long prefetch) {
        this.actual = actual;
        this.cancellations = Disposables.composite();
        this.queue = Queues.unboundedMultiproducer().get();
        this.queueBiOffer = (BiPredicate<Object, Object>) queue;
        this.resultSelector = resultSelector;
        this.condition = condition;
        ACTIVE.lazySet(this, 2);
        leftSubscriber = new ConditionalJoinChild<>(this, LEFT_VALUE, LEFT_CLOSE, prefetch);
        cancellations.add(leftSubscriber);
        rightSubscriber = new ConditionalJoinChild<>(this, RIGHT_VALUE, RIGHT_CLOSE, prefetch);
        cancellations.add(rightSubscriber);
    }

    public ConditionalJoinChild<TRight, R> getRightSubscriber() {
        return rightSubscriber;
    }

    public ConditionalJoinChild<TLeft, R> getLeftSubscriber() {
        return leftSubscriber;
    }

    @Override
    public final CoreSubscriber<? super R> actual() {
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

        leftSubscriber.getPrevious().clear();
        rightSubscriber.getPrevious().clear();

        subscriber.onError(ex);
    }

    @Override
    public void innerError(Throwable ex) {
        if (Exceptions.addThrowable(ERROR, this, ex)) {
            ACTIVE.decrementAndGet(this);
            drain();
        } else {
            Operators.onErrorDropped(ex, actual.currentContext());
        }
    }

    @Override
    public void innerComplete(Disposable child) {
        cancellations.remove(child);
        ACTIVE.decrementAndGet(this);
        drain();
    }

    @Override
    public void innerValue(Integer type, Object o) {
        queueBiOffer.test(type, o);
        drain();
    }

    void drain() {
        if (WIP.getAndIncrement(this) != 0) {
            return;
        }

        int missed = 1;
        Queue<Object> q = queue;
        Subscriber<? super R> subscriber = actual;

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

                Integer mode = (Integer) q.poll();


                boolean empty = mode == null;

                if (isActive && empty) {

                    leftSubscriber.getPrevious().clear();
                    rightSubscriber.getPrevious().clear();
                    cancellations.dispose();

                    subscriber.onComplete();
                    return;
                }

                if (empty) {
                    break;
                }

                Object val = q.poll();

                try {
                    if (mode == LEFT_VALUE) {
                        @SuppressWarnings("unchecked") TLeft left = (TLeft) val;
                        processLeft(left, subscriber);
                    }
                    if (mode == RIGHT_VALUE) {
                        @SuppressWarnings("unchecked") TRight right = (TRight) val;
                        processRight(right, subscriber);
                    }
                }
                catch (Throwable e){
                    Exceptions.addThrowable(ERROR,
                            this,
                            Operators.onOperatorError(this,
                                    e, val, actual.currentContext()));
                    errorAll(subscriber);
                    return;
                }

            }
            missed = WIP.addAndGet(this, -missed);
        } while (missed != 0);
    }

    private void processLeft(TLeft value, Subscriber<? super R> subscriber) {
        leftSubscriber.getPrevious().add(value);
        for (TRight rightValue : rightSubscriber.getPrevious()) {
            filterAndSelectResult(value, rightValue, value, subscriber);
        }
    }

    private void processRight(TRight value, Subscriber<? super R> subscriber) {
        rightSubscriber.getPrevious().add(value);
        for (TLeft leftValue : leftSubscriber.getPrevious()) {
            filterAndSelectResult(leftValue,value,value,subscriber);
        }
    }

    private void filterAndSelectResult(TLeft left,TRight right,Object value,Subscriber<? super R> subscriber){
            if (condition.test(left, right))
                subscriber.onNext(
                        Objects.requireNonNull(
                                resultSelector.apply(left, right),
                                "The resultSelector returned a null value")
                );
    }
}
