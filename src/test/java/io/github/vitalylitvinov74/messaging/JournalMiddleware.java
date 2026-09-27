package io.github.vitalylitvinov74.messaging;

import an.awesome.pipelinr.Command;
import an.awesome.pipelinr.Notification;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Промежуточный обработчик сервиса: записывает каждое сообщение, прошедшее через него к обработчику. */
public final class JournalMiddleware implements Command.Middleware, Notification.Middleware {
    private final List<Object> passed = new CopyOnWriteArrayList<>();

    @Override
    public <R, C extends Command<R>> R invoke(C command, Command.Middleware.Next<R> next) {
        this.passed.add(command);
        return next.invoke();
    }

    @Override
    public <N extends Notification> void invoke(N notification, Notification.Middleware.Next next) {
        this.passed.add(notification);
        next.invoke();
    }

    public List<Object> passed() {
        return List.copyOf(this.passed);
    }
}
