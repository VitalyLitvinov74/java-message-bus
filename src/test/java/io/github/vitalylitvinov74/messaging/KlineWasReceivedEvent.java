package io.github.vitalylitvinov74.messaging;

/** Топик берётся из настроек, ключ — компонент record. */
@Publish(topic = "${market.klines}")
public record KlineWasReceivedEvent(@PartitionKey String instrument, double close) implements DomainEvent {
}
