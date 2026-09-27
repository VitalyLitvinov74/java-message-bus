package io.github.vitalylitvinov74.messaging.kafka;

import io.github.vitalylitvinov74.messaging.DomainEvent;
import io.github.vitalylitvinov74.messaging.PartitionKey;
import io.github.vitalylitvinov74.messaging.Publish;

/** Топик берётся из настроек, ключ — компонент record. */
@Publish(topic = "${market.klines}")
public record KlineWasReceivedEvent(@PartitionKey String instrument, double close) implements DomainEvent {
}
