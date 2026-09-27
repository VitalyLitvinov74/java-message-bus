package io.github.vitalylitvinov74.messaging.kafka;

import io.github.vitalylitvinov74.messaging.DomainEvent;
import io.github.vitalylitvinov74.messaging.PartitionKey;
import io.github.vitalylitvinov74.messaging.Publish;

/** Топик записан прямо, ключ собирает метод события. */
@Publish(topic = "market.fees")
public record FeeWereLoadedEvent(String exchange, String symbol) implements DomainEvent {

    @PartitionKey
    public String key() {
        return this.exchange + ":" + this.symbol;
    }
}
