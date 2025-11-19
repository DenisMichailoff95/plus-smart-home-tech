package ru.yandex.practicum.analyzer.processors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.handlers.HubHandler;
import ru.yandex.practicum.analyzer.handlers.HubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final Consumer<String, HubEventAvro> consumer;
    private final HubHandler hubHandler;

    @Value("${topic.hub-event-topic}")
    private String topic;

    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(topic));
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            log.info("HubEventProcessor запущен и подписан на топик: {}", topic);
            Map<String, HubEventHandler> mapBuilder = hubHandler.getHandlers();

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(1000));
                log.info("Получено {} записей из топика {}", records.count(), topic);

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro event = record.value();
                    String payloadName = event.getPayload().getClass().getSimpleName();
                    log.info("Получение события хаба: тип={}, hubId={}, timestamp={}",
                            payloadName, event.getHubId(), event.getTimestamp());

                    if (mapBuilder.containsKey(payloadName)) {
                        try {
                            mapBuilder.get(payloadName).handle(event);
                            log.info("Обработчик успешно выполнен для события {}", payloadName);
                        } catch (Exception e) {
                            log.error("Ошибка в обработчике для события {}", payloadName, e);
                        }
                    } else {
                        log.error("Нет обработчика для события типа {}", payloadName);
                    }
                }
                consumer.commitSync();
            }
        } catch (WakeupException ignored) {
            log.info("HubEventProcessor остановлен");
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