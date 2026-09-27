package io.github.vitalylitvinov74.messaging.kafka;

import an.awesome.pipelinr.Command;
import an.awesome.pipelinr.Voidy;
import io.github.vitalylitvinov74.messaging.MessageBus;

/** Обработчик команды, который сам зависит от шины: так выглядит обычный обработчик сервиса. */
public final class ApplySettingsHandler implements Command.Handler<ApplySettingsCommand, Voidy> {
    private final MessageBus bus;

    public ApplySettingsHandler(MessageBus bus) {
        this.bus = bus;
    }

    @Override
    public Voidy handle(ApplySettingsCommand command) {
        this.bus.publish(new SettingsWereUpdatedEvent(command.nodeId(), command.revision()));
        return new Voidy();
    }
}
