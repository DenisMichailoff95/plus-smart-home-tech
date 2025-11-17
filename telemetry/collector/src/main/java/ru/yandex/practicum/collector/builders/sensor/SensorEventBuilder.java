package ru.yandex.practicum.collector.builders.sensor;

import ru.yandex.practicum.collector.schemas.sensorEvent.BaseSensorEvent;

public interface SensorEventBuilder {
    void builder(BaseSensorEvent event);
}
