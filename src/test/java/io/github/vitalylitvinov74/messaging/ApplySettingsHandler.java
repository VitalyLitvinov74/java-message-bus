package io.github.vitalylitvinov74.messaging;

import an.awesome.pipelinr.Command;
import an.awesome.pipelinr.Voidy;

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
