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
            log.debug("Building hub event: {}", event.getType());
            SpecificRecordBase avroEvent = toAvro(event);
            log.debug("Successfully built Avro event for hub: {}", event.getHubId());

            producer.send(avroEvent, event.getHubId(), event.getTimestamp(), topic);
            log.debug("Successfully sent hub event: {}", event.getType());

        } catch (Exception e) {
            log.error("Error building hub event - Type: {}, Hub: {}",
                    event.getType(), event.getHubId(), e);
            throw new RuntimeException("Hub event building failed: " + e.getMessage(), e);
        }
    }

    public abstract SpecificRecordBase toAvro(BaseHubEvent hubEvent);
}