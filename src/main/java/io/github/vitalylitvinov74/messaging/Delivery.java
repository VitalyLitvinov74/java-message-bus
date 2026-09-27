package io.github.vitalylitvinov74.messaging;

/**
 * Куда доставить событие при публикации. Выбирает тот, кто публикует: одно и то же событие
 * бывает нужно и внутри сервиса, и соседям, а горячему потоку фактов местные обработчики
 * только мешают.
 */
public enum Delivery {

    /** Только местным обработчикам внутри процесса. Разметка {@link Publish} не читается. */
    InMemory(true, false),

    /** Только в топик Kafka из {@link Publish}. Событие без разметки отправить некуда — это ошибка. */
    InNetwork(false, true),

    /** Местным обработчикам, затем в топик Kafka, если событие размечено {@link Publish}. */
    EveryWhere(true, true);

    private final boolean memory;
    private final boolean network;

    Delivery(boolean memory, boolean network) {
        this.memory = memory;
        this.network = network;
    }

    boolean reachesMemory() {
        return this.memory;
    }

    boolean reachesNetwork() {
        return this.network;
    }
}
