package ru.yandex.practicum;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.processor.HubEventProcessor;
import ru.yandex.practicum.processor.SnapshotProcessor;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Analyzer.class, args);

        final HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();

        snapshotProcessor.start();

        registerShutdownHook(context, hubEventProcessor, snapshotProcessor);
    }

    private static void registerShutdownHook(ConfigurableApplicationContext context,
                                             HubEventProcessor hubEventProcessor,
                                             SnapshotProcessor snapshotProcessor) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Analyzer...");
            hubEventProcessor.stop();
            snapshotProcessor.stop();
            context.close();
        }));
    }
}