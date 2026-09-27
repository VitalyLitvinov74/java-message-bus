package io.github.vitalylitvinov74.messaging.kafka;

import io.github.vitalylitvinov74.messaging.DomainEvent;
import io.github.vitalylitvinov74.messaging.PartitionKey;
import io.github.vitalylitvinov74.messaging.Publish;

@Publish(topic = "market.parameterized")
public record KeyWithParameterEvent(String instrument) implements DomainEvent {

    @PartitionKey
    public String keyFor(String exchange) {
        return exchange + ":" + this.instrument;
    }
}
