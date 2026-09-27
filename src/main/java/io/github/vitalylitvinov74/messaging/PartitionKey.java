package io.github.vitalylitvinov74.messaging;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Ключ партиции события с {@link Publish}. События с одним ключом попадают в одну партицию
 * и читаются в порядке публикации.
 *
 * Ставится на метод без параметров, который возвращает строку, либо на компонент record:
 * тогда разметка переходит на его метод доступа.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface PartitionKey {
}
