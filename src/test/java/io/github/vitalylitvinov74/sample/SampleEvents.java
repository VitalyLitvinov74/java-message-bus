package io.github.vitalylitvinov74.sample;

import io.github.vitalylitvinov74.messaging.DomainEvent;

/** Выдаёт тестам непубличное событие сервиса, которое из другого пакета не создать. */
public final class SampleEvents {

    public DomainEvent tradeOf(String instrument) {
        return new HiddenTradeEvent(instrument);
    }
}
