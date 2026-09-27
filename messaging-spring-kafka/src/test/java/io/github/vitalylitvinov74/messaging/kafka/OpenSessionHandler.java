package io.github.vitalylitvinov74.messaging.kafka;

import an.awesome.pipelinr.Command;

public final class OpenSessionHandler implements Command.Handler<OpenSessionCommand, String> {

    @Override
    public String handle(OpenSessionCommand command) {
        return "session:" + command.exchange();
    }
}
