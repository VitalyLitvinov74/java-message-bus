package io.github.vitalylitvinov74.messaging;

import an.awesome.pipelinr.Command;
import an.awesome.pipelinr.Pipeline;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Единственная точка, через которую прикладной код отправляет сообщения. Команды, запросы и
 * местные обработчики событий получает {@link Pipeline}, а событие с {@link Publish} после них
 * уходит в свой топик Kafka.
 *
 * Приём из Kafka сюда не входит: его делает {@code @KafkaListener} сервиса, который передаёт
 * принятое сообщение в эту же шину. Топик каждого типа события строится один раз и запоминается,
 * чтобы разметка не читалась на каждой публикации.
 */
public final class MessageBus {

    private final Pipeline pipeline;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Environment environment;
    private final Map<Class<?>, Optional<EventTopic>> topics = new ConcurrentHashMap<>();

    public MessageBus(
            Pipeline pipeline,
            KafkaTemplate<String, Object> kafkaTemplate,
            Environment environment
    ) {
        this.pipeline = pipeline;
        this.kafkaTemplate = kafkaTemplate;
        this.environment = environment;
    }

    /** Исполняет команду и возвращает результат её обработчика. */
    public <R, C extends Command<R>> R send(C command) {
        return this.pipeline.send(command);
    }

    /** Исполняет запрос и возвращает результат его обработчика. */
    public <R, Q extends Query<R>> R ask(Q query) {
        return this.pipeline.send(query);
    }

    /** Публикует событие везде: местным обработчикам и в топик, если он размечен. */
    public CompletionStage<Void> publish(DomainEvent event) {
        return this.publish(event, Delivery.EveryWhere);
    }

    /**
     * Отдаёт событие местным обработчикам и публикует его в топик из {@link Publish} — в той мере,
     * в какой это разрешает {@code delivery}.
     *
     * Топик находится до местных обработчиков: ошибка разметки бросается сразу, как и ошибка
     * местного обработчика, и в Kafka ничего не уходит. Отказ брокера приходит только через
     * результат: он завершается, когда брокер подтвердил запись, или с ошибкой. Без отправки
     * в топик результат уже завершён.
     */
    public CompletionStage<Void> publish(DomainEvent event, Delivery delivery) {
        Optional<EventTopic> topic = delivery.reachesNetwork()
                ? this.topics.computeIfAbsent(event.getClass(), this::topicOf)
                : Optional.empty();
        if (delivery == Delivery.InNetwork && topic.isEmpty()) {
            throw new IllegalStateException(
                    "Событие " + event.getClass().getName() + " без @Publish нельзя отправить только в сеть"
            );
        }

        if (delivery.reachesMemory()) {
            this.pipeline.send(event);
        }

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
