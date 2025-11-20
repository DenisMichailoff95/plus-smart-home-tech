package ru.yandex.practicum.analyzer.processors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.Stoppable;
import ru.yandex.practicum.analyzer.handlers.HubHandler;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable, Stoppable {

    private final Consumer<String, HubEventAvro> consumer;
    private final HubHandler hubHandler;

    @Value("${topic.hub-event-topic}")
    private String topic;

    private volatile boolean running = true;

    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(topic));
            log.info("HubEventProcessor started and subscribed to topic: {}", topic);

            while (running) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.info("Received {} hub events from topic {}", records.count(), topic);

                    for (ConsumerRecord<String, HubEventAvro> record : records) {
                        HubEventAvro event = record.value();
                        String payloadName = event.getPayload().getClass().getSimpleName();
                        log.debug("Processing hub event: type={}, hubId={}", payloadName, event.getHubId());

                        try {
                            hubHandler.handleEvent(event);
                            log.debug("Successfully processed hub event type: {}", payloadName);
                        } catch (Exception e) {
                            log.error("Error processing hub event type: {}", payloadName, e);
                        }
                    }
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            log.info("HubEventProcessor wakeup called");
        } catch (Exception e) {
            log.error("Error in HubEventProcessor", e);
        } finally {
            shutdown();
        }
    }

    @Override
    public void stop() {
        running = false;
        consumer.wakeup();
    }

    private void shutdown() {
        try {
            consumer.commitSync();
        } finally {
            consumer.close();
            log.info("HubEventProcessor stopped and consumer closed");
        }
    }
}