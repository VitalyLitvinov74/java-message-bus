package io.github.vitalylitvinov74.messaging.kafka;

import an.awesome.pipelinr.Notification;
import io.github.vitalylitvinov74.messaging.DomainEvent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Местный обработчик любого события: запоминает, что до него дошло. */
public final class ReceivedEvents implements Notification.Handler<DomainEvent> {
    private final List<DomainEvent> received = new CopyOnWriteArrayList<>();

    @Override
    public void handle(DomainEvent event) {
        this.received.add(event);
    }

    public List<DomainEvent> received() {
        return List.copyOf(this.received);
    }
}
