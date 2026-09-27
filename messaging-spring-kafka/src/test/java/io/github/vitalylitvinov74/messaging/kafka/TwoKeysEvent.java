package io.github.vitalylitvinov74.messaging.kafka;

import io.github.vitalylitvinov74.messaging.DomainEvent;
import io.github.vitalylitvinov74.messaging.PartitionKey;
import io.github.vitalylitvinov74.messaging.Publish;

@Publish(topic = "market.two-keys")
public record TwoKeysEvent(@PartitionKey String exchange, @PartitionKey String symbol) implements DomainEvent {
}
