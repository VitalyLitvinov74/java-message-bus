package io.github.vitalylitvinov74.messaging;

@Publish(topic = "market.parameterized")
public record KeyWithParameterEvent(String instrument) implements DomainEvent {

    @PartitionKey
    public String keyFor(String exchange) {
        return exchange + ":" + this.instrument;
    }
}
