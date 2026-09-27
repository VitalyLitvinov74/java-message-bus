package io.github.vitalylitvinov74.messaging;

@Publish(topic = "${market.blank}")
public record BlankTopicEvent(@PartitionKey String instrument) implements DomainEvent {
}
