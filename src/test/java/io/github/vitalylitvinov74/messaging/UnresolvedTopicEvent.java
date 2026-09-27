package io.github.vitalylitvinov74.messaging;

@Publish(topic = "${market.unknown}")
public record UnresolvedTopicEvent(@PartitionKey String instrument) implements DomainEvent {
}
