## Коротко

Шина сообщений для сервисов на `Spring Boot`. Команды и запросы выполняются внутри сервиса через `PipelinR`. События получают обработчики внутри сервиса, а размеченные события ещё и уходят в Kafka.

Модули:

| Модуль | Что внутри | Кто подключает |
| --- | --- | --- |
| `messaging-api` | `MessageBus`, `Query`, `DomainEvent`, `@Publish`, `@PartitionKey` | код сообщений и обработчиков |
| `messaging-spring-kafka` | реализация шины и автоматическая настройка `Spring Boot` | сборка приложения |

## Подключение

Библиотека пока не выложена в хранилище пакетов. Соберите её в корне репозитория:

```bash
mvn install
```

Затем добавьте зависимость в сервис:

```xml
<dependency>
    <groupId>io.github.vitalylitvinov74</groupId>
    <artifactId>messaging-spring-kafka</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Нужны `Java 21` и `Spring Boot 4.1`. Бин `MessageBus` появится сам. Если в сервисе уже есть свой `Pipeline` или `MessageBus`, библиотека возьмёт их и второй не создаст.

## Команды и запросы

Обработчик — обычный бин без разметки. `PipelinR` находит его по типу сообщения.

```java
public record StartListenCommand() implements Command<Voidy> {}

@Component
public class StartListenHandler implements Command.Handler<StartListenCommand, Voidy> {
    public Voidy handle(StartListenCommand command) { ... }
}

bus.send(new StartListenCommand());
```

Запрос реализует `Query<R>` и отправляется через `bus.ask(query)`. Команде без обработчика шина отвечает ошибкой `CommandHandlerNotFoundException`.

## События

Событие реализует `DomainEvent`. Без разметки оно остаётся внутри сервиса. С `@Publish` оно после местных обработчиков уходит в топик Kafka.

```java
@Publish(topic = "${data-receiving.klines}")
public record KlineWasReceivedEvent(@PartitionKey String instrument, double close) implements DomainEvent {}

bus.publish(new KlineWasReceivedEvent("BTCUSDT", 64000.5));
```

- `topic` — имя топика или ссылка на настройку в `${...}`.
- `@PartitionKey` — ключ партиции (раздела топика). События с одним ключом читаются в порядке отправки. Ставится на компонент записи или на метод без параметров, который возвращает `String`.
- Тело события сериализует настройка сервиса `spring.kafka.producer.value-serializer`.

## Приём из Kafka

Шина не слушает Kafka. Приём остаётся за `@KafkaListener` сервиса, который передаёт сообщение в шину:

```java
@KafkaListener(topics = "${data-receiving.streams.settings}")
public void settingsWereUpdated(SettingsWereUpdatedEvent event) {
    this.bus.publish(event);
}
```

## Что бывает при ошибках

- Нет настройки топика, пустой топик или неверный `@PartitionKey`: `publish` сразу бросает `IllegalStateException`. Местные обработчики не вызываются, в Kafka ничего не уходит.
- Упал местный обработчик: ошибка бросается из `publish`, в Kafka ничего не уходит.
- Брокер не принял запись: результат `publish` завершается ошибкой, а в лог пишется тип события, ключ и топик. Бросок исключения из `publish` при этом не происходит.
- Результат `publish` завершается успешно, когда брокер подтвердил запись. Повторную отправку шина не делает.
- Шина не связана с транзакцией базы. Публикуйте событие после того, как состояние сохранено.

## Проверка

Тесты запускаются в Docker, `Maven` на машине не нужен:

```bash
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from tests
```

Успех — строка `BUILD SUCCESS` и код выхода `0`. Один из тестов отправляет событие во встроенный брокер Kafka и читает его обратно.
