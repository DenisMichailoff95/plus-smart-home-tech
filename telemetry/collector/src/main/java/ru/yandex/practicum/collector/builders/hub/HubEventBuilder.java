package ru.yandex.practicum.collector.builders.hub;

import ru.yandex.practicum.collector.schemas.hubEvent.BaseHubEvent;

public interface HubEventBuilder {

    void builder(BaseHubEvent hubEvent);
}
