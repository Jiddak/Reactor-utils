package com.jidda.reactorUtils.join;

import com.jidda.reactorUtils.InnerProducer;
import reactor.core.Disposable;

public interface ConditionalJoinParent<R> extends InnerProducer<R> {
    void innerError(Throwable ex);

    void innerComplete(Disposable sender);

    void innerValue(Integer type,Object value);

}
