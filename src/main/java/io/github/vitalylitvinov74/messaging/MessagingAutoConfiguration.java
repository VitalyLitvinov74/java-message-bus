package io.github.vitalylitvinov74.messaging;

import an.awesome.pipelinr.Command;
import an.awesome.pipelinr.CommandHandlers;
import an.awesome.pipelinr.Notification;
import an.awesome.pipelinr.NotificationHandlers;
import an.awesome.pipelinr.Pipeline;
import an.awesome.pipelinr.Pipelinr;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Подключает шину к сервису на Spring Boot. Обработчики команд, запросов и событий берутся из
 * контекста как обычные бины и разметки не требуют: PipelinR находит обработчик по типу
 * сообщения. Собственный {@link Pipeline} или {@link MessageBus} сервиса заменяет здешний.
 */
@AutoConfiguration(after = KafkaAutoConfiguration.class)
public class MessagingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("rawtypes")
    public Pipeline pipeline(
            ObjectProvider<Command.Handler> commandHandlers,
            ObjectProvider<Notification.Handler> notificationHandlers,
            ObjectProvider<Command.Middleware> commandMiddlewares,
            ObjectProvider<Notification.Middleware> notificationMiddlewares
    ) {
        CommandHandlers commands = commandHandlers::stream;
        NotificationHandlers notifications = notificationHandlers::stream;
        Command.Middlewares commandPipeline = commandMiddlewares::orderedStream;
        Notification.Middlewares notificationPipeline = notificationMiddlewares::orderedStream;

        return new Pipelinr()
                .with(commands)
                .with(notifications)
                .with(commandPipeline)
                .with(notificationPipeline);
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageBus messageBus(
            Pipeline pipeline,
            KafkaTemplate<String, Object> kafkaTemplate,
            Environment environment
    ) {
        return new MessageBus(pipeline, kafkaTemplate, environment);
    }
}
