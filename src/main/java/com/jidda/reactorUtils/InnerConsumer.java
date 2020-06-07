package com.jidda.reactorUtils;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

//Copied from reactor core
public interface InnerConsumer<I>
        extends CoreSubscriber<I>, Scannable {

    @Override
    default String stepName() {
        // /!\ this code is duplicated in `Scannable#stepName` in order to use toString instead of simple class name

        /*
         * Strip an operator name of various prefixes and suffixes.
         * @param name the operator name, usually simpleClassName or fully-qualified classname.
         * @return the stripped operator name
         */
        String name = getClass().getSimpleName();
        if (name.contains("@") && name.contains("$")) {
            name = name
                    .substring(0, name.indexOf('$'))
                    .substring(name.lastIndexOf('.') + 1);
        }
        String stripped = OPERATOR_NAME_UNRELATED_WORDS_PATTERN
                .matcher(name)
                .replaceAll("");

        if (!stripped.isEmpty()) {
            return stripped.substring(0, 1).toLowerCase() + stripped.substring(1);
        }
        return stripped;
    }
}

