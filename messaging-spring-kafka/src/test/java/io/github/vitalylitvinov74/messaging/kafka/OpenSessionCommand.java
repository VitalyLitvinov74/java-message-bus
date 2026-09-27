package io.github.vitalylitvinov74.messaging.kafka;

import an.awesome.pipelinr.Command;

public record OpenSessionCommand(String exchange) implements Command<String> {
}
