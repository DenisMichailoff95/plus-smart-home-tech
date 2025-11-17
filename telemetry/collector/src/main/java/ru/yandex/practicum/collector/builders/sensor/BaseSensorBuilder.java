package ru.yandex.practicum.collector.builders.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.practicum.collector.producer.KafkaProducer;
import ru.yandex.practicum.collector.schemas.sensorEvent.BaseSensorEvent;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseSensorBuilder implements SensorEventBuilder {

    private final KafkaProducer producer;

    @Value("${topic.telemetry-sensors}")
    private String topic;

    @Override
    public void builder(BaseSensorEvent event) {
        try {
            SpecificRecordBase avroEvent = toAvro(event);
            producer.send(avroEvent, event.getHubId(), event.getTimestamp(), topic);
            log.debug("Successfully built and sent sensor event: {}", event.getType());
        } catch (Exception e) {
            log.error("Error building sensor event: {}", event, e);
            throw new RuntimeException("Sensor event building failed", e);
        }
    }

    public abstract SpecificRecordBase toAvro(BaseSensorEvent sensorEvent);
}