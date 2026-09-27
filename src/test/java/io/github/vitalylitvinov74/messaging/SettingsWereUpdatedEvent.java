package io.github.vitalylitvinov74.messaging;

/** Событие без {@code @Publish}: живёт только внутри процесса. */
public record SettingsWereUpdatedEvent(String nodeId, long revision) implements DomainEvent {
}
