package io.github.vitalylitvinov74.messaging.kafka;

import io.github.vitalylitvinov74.messaging.DomainEvent;
import io.github.vitalylitvinov74.messaging.PartitionKey;
import io.github.vitalylitvinov74.messaging.Publish;

@Publish(topic = "market.numeric")
public record NumericKeyEvent(@PartitionKey int shard) implements DomainEvent {
}
