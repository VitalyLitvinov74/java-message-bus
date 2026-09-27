package io.github.vitalylitvinov74.messaging;

/** Топик записан прямо, ключ собирает метод события. */
@Publish(topic = "market.fees")
public record FeeWereLoadedEvent(String exchange, String symbol) implements DomainEvent {

    @PartitionKey
    public String key() {
        return this.exchange + ":" + this.symbol;
    }
}
