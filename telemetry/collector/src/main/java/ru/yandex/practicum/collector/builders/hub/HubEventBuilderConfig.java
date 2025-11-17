package ru.yandex.practicum.collector.builders.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.collector.enums.HubEventType;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class HubEventBuilderConfig {
    private final DeviceAddedBuilder deviceAddedBuilder;
    private final DeviceRemovedBuilder deviceRemovedBuilder;
    private final ScenarioAddedBuilder scenarioAddedBuilder;
    private final ScenarioRemovedBuilder scenarioRemovedBuilder;

    @Bean
    public Map<HubEventType, HubEventBuilder> getHubEventBuilders() {
        Map<HubEventType, HubEventBuilder> hubEventBuilders = new HashMap<>();

        hubEventBuilders.put(HubEventType.DEVICE_ADDED, deviceAddedBuilder);
        hubEventBuilders.put(HubEventType.DEVICE_REMOVED, deviceRemovedBuilder);
        hubEventBuilders.put(HubEventType.SCENARIO_ADDED, scenarioAddedBuilder);
        hubEventBuilders.put(HubEventType.SCENARIO_REMOVED, scenarioRemovedBuilder);

        return hubEventBuilders;
    }
}
