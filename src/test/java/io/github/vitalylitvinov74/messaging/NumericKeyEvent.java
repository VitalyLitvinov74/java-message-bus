package io.github.vitalylitvinov74.messaging;

@Publish(topic = "market.numeric")
public record NumericKeyEvent(@PartitionKey int shard) implements DomainEvent {
}
