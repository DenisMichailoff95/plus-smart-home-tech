package ru.yandex.practicum.analyzer;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.processors.HubEventProcessor;
import ru.yandex.practicum.analyzer.processors.SnapshotProcessor;

import java.util.concurrent.ExecutorService;

@Component
@RequiredArgsConstructor
public class AnalyzerRunner implements CommandLineRunner {

    private final HubEventProcessor hubEventProcessor;
    private final SnapshotProcessor snapshotProcessor;
    private final ExecutorService executorService;

    @Override
    public void run(String... args) {
        // Запускаем процессоры в отдельных потоках
        executorService.submit(hubEventProcessor);
        executorService.submit(snapshotProcessor);

        // Добавляем shutdown hook для graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (hubEventProcessor instanceof Stoppable) {
                ((Stoppable) hubEventProcessor).stop();
            }
            if (snapshotProcessor instanceof Stoppable) {
                ((Stoppable) snapshotProcessor).stop();
            }
            executorService.shutdown();
        }));
    }
}