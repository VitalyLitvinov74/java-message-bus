package io.github.vitalylitvinov74.messaging;

@Publish(topic = "market.keyless")
public record KeylessEvent(String instrument) implements DomainEvent {
}
