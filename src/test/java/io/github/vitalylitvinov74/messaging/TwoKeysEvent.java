package io.github.vitalylitvinov74.messaging;

@Publish(topic = "market.two-keys")
public record TwoKeysEvent(@PartitionKey String exchange, @PartitionKey String symbol) implements DomainEvent {
}
