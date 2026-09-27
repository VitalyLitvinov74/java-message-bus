package io.github.vitalylitvinov74.messaging.kafka;

import io.github.vitalylitvinov74.messaging.DomainEvent;

/** Событие без {@code @Publish}: живёт только внутри процесса. */
public record SettingsWereUpdatedEvent(String nodeId, long revision) implements DomainEvent {
}
