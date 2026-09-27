package io.github.vitalylitvinov74.messaging;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Двойник KafkaTemplate: запоминает каждую отправку и отвечает подтверждением брокера,
 * отказом через результат или отказом сразу, как делает настоящий KafkaTemplate.
 * Продюсер не создаётся — соединения с брокером нет.
 */
public final class StubKafkaTemplate extends KafkaTemplate<String, Object> {
    public record Sent(String topic, String key, Object value) {
    }

    private final List<Sent> sent = new ArrayList<>();
    private RuntimeException rejection;
    private KafkaException immediateFailure;

    public StubKafkaTemplate() {
        super(new DefaultKafkaProducerFactory<>(Map.of()));
    }

    /** Брокер принимает запись, но результат завершается отказом. */
    public void rejectSends(RuntimeException rejection) {
        this.rejection = rejection;
    }

    /** Отправка падает сразу, до результата: так KafkaTemplate сообщает о немедленном отказе. */
    public void failImmediately(KafkaException failure) {
        this.immediateFailure = failure;
    }

    public List<Sent> sent() {
        return List.copyOf(this.sent);
    }

    @Override
    public CompletableFuture<SendResult<String, Object>> send(String topic, String key, Object data) {
        if (this.immediateFailure != null) {
            throw this.immediateFailure;
        }
        this.sent.add(new Sent(topic, key, data));
        if (this.rejection != null) {
            return CompletableFuture.failedFuture(this.rejection);
        }
        return CompletableFuture.completedFuture(null);
    }
}
