package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.HubEvent;
import ru.yandex.practicum.model.SensorEvent;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final EventMapperService eventMapperService;

    private static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    private static final String HUBS_TOPIC = "telemetry.hubs.v1";

    public void sendSensorEvent(SensorEvent event) {
        try {
            log.info("Starting to send sensor event: {}", event.getClass().getSimpleName());

            byte[] avroBytes = eventMapperService.toAvroBytes(event);
            log.info("Converted to AVRO: {} bytes", avroBytes.length);

            kafkaTemplate.send(SENSORS_TOPIC, avroBytes);
            log.info("Sent to Kafka topic: {}", SENSORS_TOPIC);

        } catch (Exception e) {
            log.error("Failed to send sensor event to Kafka", e);
            throw new RuntimeException("Failed to send sensor event", e);
        }
    }

    public void sendHubEvent(HubEvent event) {
        try {
            log.info("Starting to send hub event: {}", event.getClass().getSimpleName());

            byte[] avroBytes = eventMapperService.toAvroBytes(event);
            log.info("Converted to AVRO: {} bytes", avroBytes.length);

            kafkaTemplate.send(HUBS_TOPIC, avroBytes);
            log.info("Sent to Kafka topic: {}", HUBS_TOPIC);

        } catch (Exception e) {
            log.error("Failed to send hub event to Kafka", e);
            throw new RuntimeException("Failed to send hub event", e);
        }
    }
}
