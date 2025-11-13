package ru.yandex.practicum.collector.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaProducer implements AutoCloseable {

    private final Producer<String, SpecificRecordBase> producer;

    public void send(SpecificRecordBase message, String hubId, Instant timestamp, String topic) {
        try {
            ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                    topic, null, timestamp.toEpochMilli(), hubId, message);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message to topic: {}, hub: {}", topic, hubId, exception);
                } else {
                    log.debug("Successfully sent message to topic: {}, partition: {}, offset: {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            log.error("Error sending message to Kafka - Topic: {}, Hub: {}", topic, hubId, e);
            throw new RuntimeException("Kafka send failed", e);
        }
    }

    @Override
    public void close() {
        try {
            producer.flush();
            producer.close(Duration.ofSeconds(10));
            log.info("Kafka producer closed successfully");
        } catch (Exception e) {
            log.error("Error closing Kafka producer", e);
        }
    }
}