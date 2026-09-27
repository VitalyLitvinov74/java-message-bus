package io.github.vitalylitvinov74.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Топик одного типа события: адрес и ключ партиции. Разметка события проверяется при создании,
 * поэтому ошибка в ней видна до первой отправки, а не у брокера.
 *
 * Отказ брокера не теряется молча: он пишется в лог и возвращается через результат, даже если
 * {@link KafkaTemplate} бросил его сразу, а не через свой результат.
 */
final class EventTopic {
    private static final Logger LOG = LoggerFactory.getLogger(EventTopic.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String name;
    private final Method partitionKey;

    EventTopic(KafkaTemplate<String, Object> kafkaTemplate, String name, Class<?> type) {
        if (name.isBlank()) {
            throw new IllegalStateException("Пустой топик в @Publish события " + type.getName());
        }

        List<Method> keys = new ArrayList<>();
        ReflectionUtils.doWithMethods(
                type,
                keys::add,
                method -> method.isAnnotationPresent(PartitionKey.class)
        );
        if (keys.size() != 1) {
            throw new IllegalStateException(
                    "Событию " + type.getName() + " с @Publish нужен ровно один метод с @PartitionKey, найдено "
                            + keys.size()
            );
        }

        Method key = keys.getFirst();
        if (key.getParameterCount() != 0 || key.getReturnType() != String.class) {
            throw new IllegalStateException(
                    "@PartitionKey события " + type.getName() + " стоит на методе " + key.getName()
                            + ": нужен метод без параметров, который возвращает String"
            );
        }
        ReflectionUtils.makeAccessible(key);

        this.kafkaTemplate = kafkaTemplate;
        this.name = name;
        this.partitionKey = key;
    }

    CompletableFuture<Void> publish(DomainEvent event) {
        String key = (String) ReflectionUtils.invokeMethod(this.partitionKey, event);

        CompletableFuture<SendResult<String, Object>> sending;
        try {
            sending = this.kafkaTemplate.send(this.name, key, event);
        } catch (RuntimeException failure) {
            sending = CompletableFuture.failedFuture(failure);
        }

        return sending
                .whenComplete((result, failure) -> {
                    if (failure != null) {
                        LOG.error(
                                "Событие {} с ключом {} не подтверждено брокером в топике {}",
                                event.getClass().getName(),
                                key,
                                this.name,
                                failure
                        );
                    }
                })
                .thenApply(result -> null);
    }
}
