package io.github.vitalylitvinov74.messaging.kafka;

import io.github.vitalylitvinov74.messaging.MessageBus;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверка через настоящий брокер: событие с @Publish доходит до топика с ключом партиции и
 * телом, которое сериализует настройка сервиса, а результат публикации завершается только
 * после подтверждения брокера.
 */
@EmbeddedKafka(partitions = 1, topics = MessageBusKafkaTest.TOPIC)
class MessageBusKafkaTest {
    static final String TOPIC = "market.klines.100ms";

    @Test
    void publishedEventReachesTopicWithPartitionKey(EmbeddedKafkaBroker broker) {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class, MessagingAutoConfiguration.class))
                .withBean(ReceivedEvents.class)
                .withPropertyValues(
                        "spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
                        "spring.kafka.producer.value-serializer="
                                + "org.springframework.kafka.support.serializer.JacksonJsonSerializer",
                        "market.klines=" + TOPIC
                )
                .run(context -> {
                    KlineWasReceivedEvent event = new KlineWasReceivedEvent("BTCUSDT", 64_000.5);

                    context.getBean(MessageBus.class)
                            .publish(event)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS);

                    DefaultKafkaConsumerFactory<String, String> consumers = new DefaultKafkaConsumerFactory<>(
                            KafkaTestUtils.consumerProps(broker, "message-bus-test", false),
                            new StringDeserializer(),
                            new StringDeserializer()
                    );
                    try (Consumer<String, String> consumer = consumers.createConsumer()) {
                        broker.consumeFromAnEmbeddedTopic(consumer, TOPIC);
                        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, TOPIC);

                        assertThat(record.key()).isEqualTo("BTCUSDT");
                        assertThat(record.value()).contains("\"instrument\":\"BTCUSDT\"", "\"close\":64000.5");
                    }
                    assertThat(context.getBean(ReceivedEvents.class).received()).containsExactly(event);
                });
    }
}
