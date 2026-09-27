package io.github.vitalylitvinov74.messaging.kafka;

import an.awesome.pipelinr.Command;
import an.awesome.pipelinr.Pipeline;
import io.github.vitalylitvinov74.messaging.DomainEvent;
import io.github.vitalylitvinov74.messaging.MessageBus;
import io.github.vitalylitvinov74.messaging.Publish;
import io.github.vitalylitvinov74.messaging.Query;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Шина на PipelinR: команды, запросы и местные обработчики событий получает {@link Pipeline},
 * а событие с {@link Publish} после них уходит в свой топик Kafka.
 *
 * Топик каждого типа события строится один раз и запоминается, чтобы разметка не читалась на
 * каждой публикации. Тип без {@link Publish} запоминается пустым значением и в Kafka не уходит.
 */
public final class PipelinrMessageBus implements MessageBus {

    private final Pipeline pipeline;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Environment environment;
    private final Map<Class<?>, Optional<EventTopic>> topics = new ConcurrentHashMap<>();

    public PipelinrMessageBus(
            Pipeline pipeline,
            KafkaTemplate<String, Object> kafkaTemplate,
            Environment environment
    ) {
        this.pipeline = pipeline;
        this.kafkaTemplate = kafkaTemplate;
        this.environment = environment;
    }

    @Override
    public <R, C extends Command<R>> R send(C command) {
        return this.pipeline.send(command);
    }

    @Override
    public <R, Q extends Query<R>> R ask(Q query) {
        return this.pipeline.send(query);
    }

    /**
     * Топик находится до местных обработчиков: неверная разметка не должна оставить событие
     * обработанным здесь и неотправленным наружу.
     */
    @Override
    public CompletionStage<Void> publish(DomainEvent event) {
        Optional<EventTopic> topic = this.topics.computeIfAbsent(event.getClass(), this::topicOf);

        this.pipeline.send(event);

        return topic
                .map(value -> value.publish(event))
                .orElseGet(() -> CompletableFuture.completedFuture(null));
    }

    /** Переводит разметку {@link Publish} в топик, подставляя настройки вместо {@code ${...}}. */
    private Optional<EventTopic> topicOf(Class<?> type) {
        Publish publish = type.getAnnotation(Publish.class);
        if (publish == null) {
            return Optional.empty();
        }

        String name;
        try {
            name = this.environment.resolveRequiredPlaceholders(publish.topic());
        } catch (IllegalArgumentException unresolved) {
            throw new IllegalStateException(
                    "Топик события " + type.getName() + " не найден в настройках: " + publish.topic(),
                    unresolved
            );
        }
        return Optional.of(new EventTopic(this.kafkaTemplate, name, type));
    }
}
