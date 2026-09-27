package io.github.vitalylitvinov74.messaging;

import an.awesome.pipelinr.Notification;

/**
 * Событие — факт, который уже произошёл. Его получают местные обработчики
 * {@code Notification.Handler}, а событие с {@link Publish} уходит ещё и в Kafka.
 */
public interface DomainEvent extends Notification {
}
