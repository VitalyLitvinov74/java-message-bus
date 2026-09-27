package io.github.vitalylitvinov74.messaging.kafka;

import an.awesome.pipelinr.Command;
import an.awesome.pipelinr.CommandHandlerNotFoundException;
import an.awesome.pipelinr.CommandHandlers;
import an.awesome.pipelinr.Notification;
import an.awesome.pipelinr.NotificationHandlers;
import an.awesome.pipelinr.Pipelinr;
import io.github.vitalylitvinov74.messaging.DomainEvent;
import io.github.vitalylitvinov74.messaging.MessageBus;
import io.github.vitalylitvinov74.sample.SampleEvents;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.KafkaException;
import org.springframework.mock.env.MockEnvironment;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Шина отдаёт команды и запросы их обработчикам, а событие — местным обработчикам и затем в
 * топик Kafka по разметке события. Ошибка разметки или местного обработчика не должна
 * отправить событие наружу, а отказ брокера не должен пройти молча.
 */
@ExtendWith(OutputCaptureExtension.class)
class PipelinrMessageBusTest {
    private static final Duration WAIT = Duration.ofSeconds(1);

    private final StubKafkaTemplate kafkaTemplate = new StubKafkaTemplate();
    private final ReceivedEvents received = new ReceivedEvents();
    private final MockEnvironment environment = new MockEnvironment()
            .withProperty("market.klines", "market.klines.100ms")
            .withProperty("market.blank", " ");

    @Test
    void sendReturnsResultOfCommandHandler() {
        MessageBus bus = this.bus(new OpenSessionHandler());

        String session = bus.send(new OpenSessionCommand("Binance"));

        assertThat(session).isEqualTo("session:Binance");
    }

    /** Команда без обработчика — ошибка, а не тихий успех. */
    @Test
    void sendFailsWhenCommandHasNoHandler() {
        MessageBus bus = this.bus(new InstrumentCountHandler());

        assertThatThrownBy(() -> bus.send(new OpenSessionCommand("Binance")))
                .isInstanceOf(CommandHandlerNotFoundException.class);
    }

    @Test
    void askReturnsResultOfQueryHandler() {
        MessageBus bus = this.bus(new OpenSessionHandler(), new InstrumentCountHandler());

        Integer count = bus.ask(new InstrumentCountQuery("Bybit"));

        assertThat(count).isEqualTo(305);
    }

    /** Событие без @Publish остаётся внутри процесса. */
    @Test
    void publishDeliversUnmarkedEventToLocalHandlersOnly() {
        MessageBus bus = this.bus(this.received);
        SettingsWereUpdatedEvent event = new SettingsWereUpdatedEvent("node-1", 7);

        CompletionStage<Void> publication = bus.publish(event);

        assertThat(publication.toCompletableFuture()).isCompleted();
        assertThat(this.received.received()).containsExactly(event);
        assertThat(this.kafkaTemplate.sent()).isEmpty();
    }

    /** Топик берётся из настроек, ключ — из компонента record, и местные обработчики тоже получают событие. */
    @Test
    void publishSendsMarkedEventToResolvedTopicWithRecordComponentKey() {
        MessageBus bus = this.bus(this.received);
        KlineWasReceivedEvent event = new KlineWasReceivedEvent("BTCUSDT", 64_000.5);

        CompletionStage<Void> publication = bus.publish(event);

        assertThat(publication.toCompletableFuture()).isCompleted();
        assertThat(this.received.received()).containsExactly(event);
        assertThat(this.kafkaTemplate.sent()).containsExactly(
                new StubKafkaTemplate.Sent("market.klines.100ms", "BTCUSDT", event)
        );
    }

    @Test
    void publishTakesKeyFromAnnotatedMethodAndLiteralTopic() {
        MessageBus bus = this.bus();
        FeeWereLoadedEvent event = new FeeWereLoadedEvent("Okx", "ETHUSDT");

        bus.publish(event);

        assertThat(this.kafkaTemplate.sent()).containsExactly(
                new StubKafkaTemplate.Sent("market.fees", "Okx:ETHUSDT", event)
        );
    }

    /** Топик типа запоминается, а ключ каждый раз читается из нового события. */
    @Test
    void publishUsesKeyOfEachEventWhenTypeIsPublishedAgain() {
        MessageBus bus = this.bus();

        bus.publish(new KlineWasReceivedEvent("BTCUSDT", 1));
        bus.publish(new KlineWasReceivedEvent("ETHUSDT", 2));

        assertThat(this.kafkaTemplate.sent())
                .extracting(StubKafkaTemplate.Sent::key)
                .containsExactly("BTCUSDT", "ETHUSDT");
    }

    /** Местные обработчики идут первыми: их отказ оставляет событие неотправленным. */
    @Test
    void publishSendsNothingWhenLocalHandlerFails() {
        MessageBus bus = this.bus(new RejectingKlineHandler());

        assertThatThrownBy(() -> bus.publish(new KlineWasReceivedEvent("BTCUSDT", 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("BTCUSDT");

        assertThat(this.kafkaTemplate.sent()).isEmpty();
    }

    /** Отказ брокера приходит через результат и остаётся в логе, даже если результат никто не ждёт. */
    @Test
    void publishCompletesExceptionallyAndLogsWhenBrokerRejects(CapturedOutput output) {
        this.kafkaTemplate.rejectSends(new IllegalStateException("брокер не подтвердил запись"));
        MessageBus bus = this.bus();

        CompletionStage<Void> publication = bus.publish(new KlineWasReceivedEvent("BTCUSDT", 1));

        assertThat(publication.toCompletableFuture())
                .failsWithin(WAIT)
                .withThrowableOfType(ExecutionException.class)
                .withMessageContaining("брокер не подтвердил запись");
        assertThat(output).contains(KlineWasReceivedEvent.class.getName(), "BTCUSDT", "market.klines.100ms");
    }

    /**
     * KafkaTemplate может отказать сразу, а не через результат. Шина сводит это к тому же отказу
     * результата: публикация без ожидания не должна ронять поток, который её вызвал.
     */
    @Test
    void publishCompletesExceptionallyWhenSendFailsImmediately(CapturedOutput output) {
        this.kafkaTemplate.failImmediately(new KafkaException("Send failed"));
        MessageBus bus = this.bus(this.received);
        KlineWasReceivedEvent event = new KlineWasReceivedEvent("BTCUSDT", 1);

        CompletionStage<Void> publication = bus.publish(event);

        assertThat(publication.toCompletableFuture())
                .failsWithin(WAIT)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(KafkaException.class);
        assertThat(this.received.received()).containsExactly(event);
        assertThat(output).contains(KlineWasReceivedEvent.class.getName(), "BTCUSDT");
    }

    /** Неизвестная настройка топика — ошибка до местных обработчиков и до брокера. */
    @Test
    void publishFailsBeforeAnyEffectWhenTopicSettingIsMissing() {
        MessageBus bus = this.bus(this.received);

        assertThatThrownBy(() -> bus.publish(new UnresolvedTopicEvent("BTCUSDT")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(UnresolvedTopicEvent.class.getName())
                .hasMessageContaining("${market.unknown}");

        assertThat(this.received.received()).isEmpty();
        assertThat(this.kafkaTemplate.sent()).isEmpty();
    }

    @Test
    void publishFailsWhenTopicSettingIsBlank() {
        MessageBus bus = this.bus(this.received);

        assertThatThrownBy(() -> bus.publish(new BlankTopicEvent("BTCUSDT")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Пустой топик")
                .hasMessageContaining(BlankTopicEvent.class.getName());

        assertThat(this.received.received()).isEmpty();
        assertThat(this.kafkaTemplate.sent()).isEmpty();
    }

    /** Без ключа события одного инструмента разлетелись бы по партициям и потеряли порядок. */
    @Test
    void publishFailsWhenMarkedEventHasNoPartitionKey() {
        MessageBus bus = this.bus(this.received);

        assertThatThrownBy(() -> bus.publish(new KeylessEvent("BTCUSDT")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(KeylessEvent.class.getName())
                .hasMessageContaining("@PartitionKey");

        assertThat(this.received.received()).isEmpty();
        assertThat(this.kafkaTemplate.sent()).isEmpty();
    }

    /** Из двух ключей шина не выбирает сама: порядок событий зависел бы от порядка методов в рефлексии. */
    @Test
    void publishFailsWhenMarkedEventHasTwoPartitionKeys() {
        MessageBus bus = this.bus(this.received);

        assertThatThrownBy(() -> bus.publish(new TwoKeysEvent("Binance", "BTCUSDT")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(TwoKeysEvent.class.getName())
                .hasMessageContaining("найдено 2");

        assertThat(this.received.received()).isEmpty();
        assertThat(this.kafkaTemplate.sent()).isEmpty();
    }

    /** Событие сервиса без модификатора public из чужого пакета публикуется так же, как публичное. */
    @Test
    void publishSendsEventOfNonPublicTypeFromAnotherPackage() {
        MessageBus bus = this.bus();
        DomainEvent event = new SampleEvents().tradeOf("BTCUSDT");

        CompletionStage<Void> publication = bus.publish(event);

        assertThat(publication.toCompletableFuture()).isCompleted();
        assertThat(this.kafkaTemplate.sent()).containsExactly(
                new StubKafkaTemplate.Sent("market.trades", "BTCUSDT", event)
        );
    }

    @Test
    void publishFailsWhenPartitionKeyMethodTakesParameters() {
        MessageBus bus = this.bus();

        assertThatThrownBy(() -> bus.publish(new KeyWithParameterEvent("BTCUSDT")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("keyFor");

        assertThat(this.kafkaTemplate.sent()).isEmpty();
    }

    @Test
    void publishFailsWhenPartitionKeyIsNotString() {
        MessageBus bus = this.bus();

        assertThatThrownBy(() -> bus.publish(new NumericKeyEvent(3)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shard");

        assertThat(this.kafkaTemplate.sent()).isEmpty();
    }

    /** Собирает шину так же, как автоматическая настройка: обработчики раскладываются по видам. */
    @SuppressWarnings("rawtypes")
    private MessageBus bus(Object... handlers) {
        List<Command.Handler> commands = Arrays.stream(handlers)
                .filter(Command.Handler.class::isInstance)
                .map(Command.Handler.class::cast)
                .toList();
        List<Notification.Handler> notifications = Arrays.stream(handlers)
                .filter(Notification.Handler.class::isInstance)
                .map(Notification.Handler.class::cast)
                .toList();
        CommandHandlers commandHandlers = commands::stream;
        NotificationHandlers notificationHandlers = notifications::stream;

        return new PipelinrMessageBus(
                new Pipelinr()
                        .with(commandHandlers)
                        .with(notificationHandlers),
                this.kafkaTemplate,
                this.environment
        );
    }
}
