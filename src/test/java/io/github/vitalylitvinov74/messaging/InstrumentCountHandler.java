package io.github.vitalylitvinov74.messaging;

import an.awesome.pipelinr.Command;

import java.util.Map;

public final class InstrumentCountHandler implements Command.Handler<InstrumentCountQuery, Integer> {
    private final Map<String, Integer> counts = Map.of("Binance", 412, "Bybit", 305);

    @Override
    public Integer handle(InstrumentCountQuery query) {
        return this.counts.get(query.exchange());
    }
}
