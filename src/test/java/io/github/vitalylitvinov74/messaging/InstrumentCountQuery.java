package io.github.vitalylitvinov74.messaging;

public record InstrumentCountQuery(String exchange) implements Query<Integer> {
}
