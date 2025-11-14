package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.builders.hub.HubEventBuilder;
import ru.yandex.practicum.collector.builders.sensor.SensorEventBuilder;
import ru.yandex.practicum.collector.enums.HubEventType;
import ru.yandex.practicum.collector.enums.SensorEventType;
import ru.yandex.practicum.collector.schemas.hubEvent.BaseHubEvent;
import ru.yandex.practicum.collector.schemas.sensorEvent.BaseSensorEvent;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class CollectorServiceImpl implements CollectorService {

    private final Map<SensorEventType, SensorEventBuilder> sensorEventBuilders;
    private final Map<HubEventType, HubEventBuilder> hubEventBuilders;

    @Override
    public void collectSensorEvent(BaseSensorEvent sensor) {
        try {
            log.info("Processing sensor event - Type: {}, Hub: {}, Sensor: {}",
                    sensor.getType(), sensor.getHubId(), sensor.getId());

            SensorEventBuilder builder = sensorEventBuilders.get(sensor.getType());
            if (builder == null) {
                throw new IllegalArgumentException("No builder found for sensor type: " + sensor.getType());
            }

            builder.builder(sensor);
            log.debug("Successfully processed sensor event: {}", sensor);

        } catch (Exception e) {
            log.error("Failed to process sensor event - Type: {}, Hub: {}, Sensor: {}. Error: {}",
                    sensor.getType(), sensor.getHubId(), sensor.getId(), e.getMessage(), e);
            throw new RuntimeException("Sensor event processing failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void collectHubEvent(BaseHubEvent hub) {
        try {
            log.info("Processing hub event - Type: {}, Hub: {}",
                    hub.getType(), hub.getHubId());

            HubEventBuilder builder = hubEventBuilders.get(hub.getType());
            if (builder == null) {
                throw new IllegalArgumentException("No builder found for hub type: " + hub.getType());
            }

            builder.builder(hub);
            log.debug("Successfully processed hub event: {}", hub);

        } catch (Exception e) {
            log.error("Failed to process hub event - Type: {}, Hub: {}. Error: {}",
                    hub.getType(), hub.getHubId(), e.getMessage(), e);
            throw new RuntimeException("Hub event processing failed: " + e.getMessage(), e);
        }
    }
}