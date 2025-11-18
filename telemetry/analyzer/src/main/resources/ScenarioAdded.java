package ru.yandex.practicum.analyzer.handlers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.Action;
import ru.yandex.practicum.analyzer.model.Condition;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.repository.*;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAdded implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final SensorRepository sensorRepository;

    @Override
    @Transactional
    public void handle(HubEventAvro hubEvent) {
        ScenarioAddedEventAvro scenarioAddedEvent = (ScenarioAddedEventAvro) hubEvent.getPayload();
        Scenario scenario = scenarioRepository.findByHubIdAndName(hubEvent.getHubId(),
                scenarioAddedEvent.getName()).orElseGet(() -> scenarioRepository.save(buildToScenario(hubEvent)));

        if (checkSensorsInScenarioConditions(scenarioAddedEvent, hubEvent.getHubId())) {
            conditionRepository.saveAll(buildToCondition(scenarioAddedEvent, scenario));
        }
        if (checkSensorsInScenarioActions(scenarioAddedEvent, hubEvent.getHubId())) {
            actionRepository.saveAll(buildToAction(scenarioAddedEvent, scenario));
        }
    }

    @Override
    public String getMessageType() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    private Scenario buildToScenario(HubEventAvro hubEvent) {
        ScenarioAddedEventAvro scenarioAddedEvent = (ScenarioAddedEventAvro) hubEvent.getPayload();
        return Scenario.builder()
                .name(scenarioAddedEvent.getName())
                .hubId(hubEvent.getHubId())
                .build();
    }

    private Set<Condition> buildToCondition(ScenarioAddedEventAvro scenarioAddedEvent, Scenario scenario) {
        return scenarioAddedEvent.getConditions().stream()
                .map(c -> {
                    Condition.ConditionBuilder builder = Condition.builder()
                            .sensor(sensorRepository.findById(c.getSensorId()).orElseThrow())
                            .scenario(scenario)
                            .type(c.getType())
                            .operation(c.getOperation());

                    // Обработка значения в зависимости от типа
                    if (c.getValue() instanceof Integer) {
                        builder.value((Integer) c.getValue());
                    } else if (c.getValue() instanceof Boolean) {
                        // Конвертируем boolean в int (true -> 1, false -> 0)
                        builder.value((Boolean) c.getValue() ? 1 : 0);
                    } else {
                        builder.value(null);
                    }

                    return builder.build();
                })
                .collect(Collectors.toSet());
    }

    private Set<Action> buildToAction(ScenarioAddedEventAvro scenarioAddedEvent, Scenario scenario) {
        return scenarioAddedEvent.getActions().stream()
                .map(action -> {
                    Action.ActionBuilder builder = Action.builder()
                            .sensor(sensorRepository.findById(action.getSensorId()).orElseThrow())
                            .scenario(scenario)
                            .type(action.getType());

                    if (action.getValue() != null) {
                        builder.value(action.getValue());
                    } else {
                        builder.value(null);
                    }

                    return builder.build();
                })
                .collect(Collectors.toSet());
    }

    private Boolean checkSensorsInScenarioConditions(ScenarioAddedEventAvro scenarioAddedEvent, String hubId) {
        return sensorRepository.existsByIdInAndHubId(scenarioAddedEvent.getConditions().stream()
                .map(ScenarioConditionAvro::getSensorId)
                .toList(), hubId);
    }

    private Boolean checkSensorsInScenarioActions(ScenarioAddedEventAvro scenarioAddedEvent, String hubId) {
        return sensorRepository.existsByIdInAndHubId(scenarioAddedEvent.getActions().stream()
                .map(DeviceActionAvro::getSensorId)
                .toList(), hubId);
    }
}