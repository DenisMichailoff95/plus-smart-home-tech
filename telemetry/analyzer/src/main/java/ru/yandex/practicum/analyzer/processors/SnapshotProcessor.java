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
import ru.yandex.practicum.analyzer.handlers.SnapshotHandler;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor implements Runnable, Stoppable {

    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final SnapshotHandler snapshotHandler;

    @Value("${topic.snapshots-topic}")
    private String topic;

    private volatile boolean running = true;

    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(topic));
            log.info("SnapshotProcessor started and subscribed to topic: {}", topic);

            while (running) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.info("Received {} snapshots from topic {}", records.count(), topic);

                    for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                        SensorsSnapshotAvro snapshot = record.value();
                        log.debug("Processing snapshot: hubId={}, sensorsCount={}",
                                snapshot.getHubId(), snapshot.getSensorsState().size());

                        try {
                            snapshotHandler.buildSnapshot(snapshot);
                            log.debug("Successfully processed snapshot for hub: {}", snapshot.getHubId());
                        } catch (Exception e) {
                            log.error("Error processing snapshot for hub: {}", snapshot.getHubId(), e);
                        }
                    }
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            log.info("SnapshotProcessor wakeup called");
        } catch (Exception e) {
            log.error("Error in SnapshotProcessor", e);
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
            log.info("SnapshotProcessor stopped and consumer closed");
        }
    }
}