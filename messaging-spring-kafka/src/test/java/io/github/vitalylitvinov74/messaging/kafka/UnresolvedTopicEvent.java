package io.github.vitalylitvinov74.messaging.kafka;

import io.github.vitalylitvinov74.messaging.DomainEvent;
import io.github.vitalylitvinov74.messaging.PartitionKey;
import io.github.vitalylitvinov74.messaging.Publish;

@Publish(topic = "${market.unknown}")
public record UnresolvedTopicEvent(@PartitionKey String instrument) implements DomainEvent {
}
