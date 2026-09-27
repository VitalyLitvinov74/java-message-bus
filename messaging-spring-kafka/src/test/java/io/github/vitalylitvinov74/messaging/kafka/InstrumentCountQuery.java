package io.github.vitalylitvinov74.messaging.kafka;

import io.github.vitalylitvinov74.messaging.Query;

public record InstrumentCountQuery(String exchange) implements Query<Integer> {
}
