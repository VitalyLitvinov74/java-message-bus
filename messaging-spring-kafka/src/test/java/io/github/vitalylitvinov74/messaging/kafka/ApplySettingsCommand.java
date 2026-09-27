package io.github.vitalylitvinov74.messaging.kafka;

import an.awesome.pipelinr.Command;
import an.awesome.pipelinr.Voidy;

public record ApplySettingsCommand(String nodeId, long revision) implements Command<Voidy> {
}
