package io.github.vitalylitvinov74.messaging.kafka;

import an.awesome.pipelinr.CommandHandlers;
import an.awesome.pipelinr.Pipeline;
import an.awesome.pipelinr.Pipelinr;
import io.github.vitalylitvinov74.messaging.MessageBus;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.mock.env.MockEnvironment;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Сервис подключает шину одной зависимостью: обработчики — обычные бины без разметки,
 * а собственный Pipeline или MessageBus сервиса не получает двойника.
 */
class MessagingAutoConfigurationTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class, MessagingAutoConfiguration.class));

    /** Обработчик команды зависит от шины и сам публикует событие: цикла зависимостей нет. */
    @Test
    void routesMessagesToHandlerBeans() {
        this.context
                .withBean(ApplySettingsHandler.class)
                .withBean(InstrumentCountHandler.class)
                .withBean(ReceivedEvents.class)
                .run(context -> {
                    MessageBus bus = context.getBean(MessageBus.class);

                    bus.send(new ApplySettingsCommand("node-1", 3));
                    Integer count = bus.ask(new InstrumentCountQuery("Binance"));

                    assertThat(count).isEqualTo(412);
                    assertThat(context.getBean(ReceivedEvents.class).received())
                            .containsExactly(new SettingsWereUpdatedEvent("node-1", 3));
                });
    }

    /** Промежуточные обработчики сервиса — обычные бины, и через них проходят и команды, и события. */
    @Test
    void passesMessagesThroughMiddlewareBeans() {
        this.context
                .withBean(ApplySettingsHandler.class)
                .withBean(ReceivedEvents.class)
                .withBean(JournalMiddleware.class)
                .run(context -> {
                    ApplySettingsCommand command = new ApplySettingsCommand("node-1", 4);

                    context.getBean(MessageBus.class).send(command);

                    assertThat(context.getBean(JournalMiddleware.class).passed())
                            .containsExactly(command, new SettingsWereUpdatedEvent("node-1", 4));
                });
    }

    /** Сервис со своим Pipeline не получает второй: шина работает через Pipeline сервиса. */
    @Test
    void usesPipelineOfService() {
        CommandHandlers handlers = () -> Stream.of(new OpenSessionHandler());
        Pipeline own = new Pipelinr().with(handlers);

        this.context
                .withBean("servicePipeline", Pipeline.class, () -> own)
                .run(context -> {
                    assertThat(context).hasSingleBean(Pipeline.class);
                    assertThat(context.getBean(MessageBus.class).send(new OpenSessionCommand("Okx")))
                            .isEqualTo("session:Okx");
                });
    }

    @Test
    void keepsMessageBusOfService() {
        MessageBus own = new PipelinrMessageBus(new Pipelinr(), new StubKafkaTemplate(), new MockEnvironment());

        this.context
                .withBean("serviceBus", MessageBus.class, () -> own)
                .run(context -> {
                    assertThat(context).hasSingleBean(MessageBus.class);
                    assertThat(context.getBean(MessageBus.class)).isSameAs(own);
                });
    }
}
