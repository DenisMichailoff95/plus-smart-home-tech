package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.collector.schemas.hubEvent.BaseHubEvent;
import ru.yandex.practicum.collector.schemas.sensorEvent.BaseSensorEvent;

public interface CollectorService {

    void collectSensorEvent(BaseSensorEvent sensor);

    void collectHubEvent(BaseHubEvent hub);
}
