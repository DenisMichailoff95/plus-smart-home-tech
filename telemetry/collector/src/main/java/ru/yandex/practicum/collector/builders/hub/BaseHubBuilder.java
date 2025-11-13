package ru.yandex.practicum.collector.builders.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.practicum.collector.producer.KafkaProducer;
import ru.yandex.practicum.collector.schemas.hubEvent.BaseHubEvent;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseHubBuilder implements HubEventBuilder {

    private final KafkaProducer producer;

    @Value("${topic.telemetry-hubs}")
    private String topic;

    @Override
    public void builder(BaseHubEvent event) {
        try {
            SpecificRecordBase avroEvent = toAvro(event);
            producer.send(avroEvent, event.getHubId(), event.getTimestamp(), topic);
            log.debug("Successfully built and sent hub event: {}", event.getType());
        } catch (Exception e) {
            log.error("Error building hub event: {}", event, e);
            throw new RuntimeException("Hub event building failed", e);
        }
    }

    public abstract SpecificRecordBase toAvro(BaseHubEvent hubEvent);
}