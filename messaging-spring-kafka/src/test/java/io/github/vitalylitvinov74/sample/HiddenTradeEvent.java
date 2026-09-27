package io.github.vitalylitvinov74.sample;

import io.github.vitalylitvinov74.messaging.DomainEvent;
import io.github.vitalylitvinov74.messaging.PartitionKey;
import io.github.vitalylitvinov74.messaging.Publish;

/** Событие сервиса без модификатора public: шина обязана прочитать его ключ из чужого пакета. */
@Publish(topic = "market.trades")
record HiddenTradeEvent(@PartitionKey String instrument) implements DomainEvent {
}
