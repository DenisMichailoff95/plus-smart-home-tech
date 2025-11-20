package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.service.HubEventService;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final HubEventService hubEventService;
    private volatile boolean running = true;
    private KafkaConsumer<String, byte[]> consumer;

    private static final String HUBS_TOPIC = "telemetry.hubs.v1";
    private static final String CONSUMER_GROUP = "analyzer-hub-events";

    @Override
    public void run() {
        log.info("Starting Hub Event Processor...");
        initializeConsumer();

        try {
            consumer.subscribe(List.of(HUBS_TOPIC));
            log.info("Subscribed to topic: {}", HUBS_TOPIC);

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
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", CONSUMER_GROUP);
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