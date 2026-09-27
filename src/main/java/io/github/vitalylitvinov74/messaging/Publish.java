package io.github.vitalylitvinov74.messaging;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Событие публикуется в топик Kafka после местных обработчиков. Адрес факта задаёт его
 * владелец, поэтому разметка стоит на классе события, а не у получателей.
 *
 * Событию с этой разметкой нужен ровно один метод с {@link PartitionKey}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Publish {

    /** Топик Kafka. Можно сослаться на настройку: {@code "${data-receiving.klines}"}. */
    String topic();
}
