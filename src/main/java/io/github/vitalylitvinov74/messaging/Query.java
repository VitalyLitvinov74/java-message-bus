package io.github.vitalylitvinov74.messaging;

import an.awesome.pipelinr.Command;

/**
 * Запрос на чтение. Технически это команда PipelinR, но контракт другой: запрос не меняет
 * состояние и всегда возвращает результат. Отправляется через {@link MessageBus#ask}.
 */
public interface Query<R> extends Command<R> {
}
