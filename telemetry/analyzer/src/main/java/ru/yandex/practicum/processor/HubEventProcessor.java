package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.config.KafkaConfig;
import ru.yandex.practicum.service.HubEventService;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final HubEventService hubEventService;
    private final KafkaConfig kafkaConfig;

    private volatile boolean running = true;
    private KafkaConsumer<String, byte[]> consumer;

    @Override
    public void run() {
        log.info("Starting Hub Event Processor...");
        initializeConsumer();

        try {
            String hubsTopic = "telemetry.hubs.v1";
            String consumerGroup = "analyzer-hub-events";

            consumer.subscribe(List.of(hubsTopic));
            log.info("Subscribed to topic: {}", hubsTopic);
            log.info("Consumer group: {}", consumerGroup);
            log.info("Bootstrap servers: {}", kafkaConfig.getBootstrapServers());

            while (running) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.debug("Received {} hub events", records.count());
                    processHubEvents(records);
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            log.error("Error in hub event processor", e);
        } finally {
            shutdown();
        }
    }

    private void initializeConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConfig.getBootstrapServers());
        props.put("group.id", "analyzer-hub-events");  // Можно использовать kafkaConfig.getConsumer().getGroupId() если настроить
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");

        consumer = new KafkaConsumer<>(props);
    }

    private void processHubEvents(ConsumerRecords<String, byte[]> records) {
        for (ConsumerRecord<String, byte[]> record : records) {
            try {
                hubEventService.processHubEvent(record.value());
                log.debug("Processed hub event from topic: {}", record.topic());
            } catch (Exception e) {
                log.error("Error processing hub event", e);
            }
        }
    }

    public void stop() {
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    private void shutdown() {
        if (consumer != null) {
            consumer.close();
        }
        log.info("Hub Event Processor stopped");
    }
}