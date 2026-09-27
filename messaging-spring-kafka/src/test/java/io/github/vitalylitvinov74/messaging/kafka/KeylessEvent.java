package io.github.vitalylitvinov74.messaging.kafka;

import io.github.vitalylitvinov74.messaging.DomainEvent;
import io.github.vitalylitvinov74.messaging.Publish;

@Publish(topic = "market.keyless")
public record KeylessEvent(String instrument) implements DomainEvent {
}
