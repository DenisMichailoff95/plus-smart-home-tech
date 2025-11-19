package ru.yandex.practicum.analyzer.processors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.handlers.SnapshotHandler;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor implements Runnable {

    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final SnapshotHandler snapshotHandler;

    @Value("${topic.snapshots-topic}")
    private String topic;

    public void run() {
        try {
            consumer.subscribe(List.of(topic));
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            log.info("SnapshotProcessor запущен и подписан на топик: {}", topic);

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    log.info("Получено {} записей из топика {}", records.count(), topic);
                }

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    SensorsSnapshotAvro sensorsSnapshot = record.value();
                    log.info("Обработка снапшота: hubId={}, timestamp={}, sensors={}",
                            sensorsSnapshot.getHubId(),
                            sensorsSnapshot.getTimestamp(),
                            sensorsSnapshot.getSensorsState().keySet());
                    snapshotHandler.buildSnapshot(sensorsSnapshot);
                }
                consumer.commitSync();
            }
        } catch (WakeupException ignored) {
            log.info("SnapshotProcessor остановлен");
        } catch (Exception e) {
            log.error("Ошибка получения данных из топика {}", topic, e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
                log.info("Consumer закрыт");
            }
        }
    }
}