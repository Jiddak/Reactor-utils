package com.jidda.reactorUtils.intersect;
import com.jidda.reactorUtils.InnerProducer;

interface IntersectParent<T> extends InnerProducer<T> {
    void innerError(Throwable ex);

    void innerComplete(IntersectChild<T> sender);

    void innerValue(Integer id, T o);
}
