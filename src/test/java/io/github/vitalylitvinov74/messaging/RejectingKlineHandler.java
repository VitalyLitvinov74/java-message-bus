package io.github.vitalylitvinov74.messaging;

import an.awesome.pipelinr.Notification;

/** Местный обработчик свечи, который отказывает. */
public final class RejectingKlineHandler implements Notification.Handler<KlineWasReceivedEvent> {

    @Override
    public void handle(KlineWasReceivedEvent event) {
        throw new IllegalStateException("Свеча " + event.instrument() + " отвергнута");
    }
}
