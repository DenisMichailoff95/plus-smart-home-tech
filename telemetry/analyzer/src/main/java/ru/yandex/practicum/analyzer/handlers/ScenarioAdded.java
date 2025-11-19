package ru.yandex.practicum.analyzer.handlers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.Action;
import ru.yandex.practicum.analyzer.model.Condition;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.model.Sensor;
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
        String hubId = hubEvent.getHubId();
        String scenarioName = scenarioAddedEvent.getName();

        log.info("Обработка добавления сценария: hub={}, name={}", hubId, scenarioName);

        // Создаем или получаем сценарий
        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                .orElseGet(() -> {
                    Scenario newScenario = buildToScenario(hubEvent);
                    return scenarioRepository.save(newScenario);
                });

        // Сохраняем условия
        Set<Condition> conditions = buildToCondition(scenarioAddedEvent, scenario);
        conditionRepository.saveAll(conditions);
        log.info("Сохранено {} условий для сценария '{}'", conditions.size(), scenarioName);

        // Сохраняем действия
        Set<Action> actions = buildToAction(scenarioAddedEvent, scenario);
        actionRepository.saveAll(actions);
        log.info("Сохранено {} действий для сценария '{}'", actions.size(), scenarioName);
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
                    Optional<Sensor> sensorOpt = sensorRepository.findById(c.getSensorId());
                    if (sensorOpt.isEmpty()) {
                        log.warn("Датчик {} не найден при создании условия", c.getSensorId());
                        throw new IllegalArgumentException("Датчик не найден: " + c.getSensorId());
                    }

                    return Condition.builder()
                            .sensor(sensorOpt.get())
                            .scenario(scenario)
                            .type(c.getType())
                            .operation(c.getOperation())
                            .value(setValue(c.getValue()))
                            .build();
                })
                .collect(Collectors.toSet());
    }

    private Set<Action> buildToAction(ScenarioAddedEventAvro scenarioAddedEvent, Scenario scenario) {
        return scenarioAddedEvent.getActions().stream()
                .map(action -> {
                    Optional<Sensor> sensorOpt = sensorRepository.findById(action.getSensorId());
                    if (sensorOpt.isEmpty()) {
                        log.warn("Датчик {} не найден при создании действия", action.getSensorId());
                        throw new IllegalArgumentException("Датчик не найден: " + action.getSensorId());
                    }

                    return Action.builder()
                            .sensor(sensorOpt.get())
                            .scenario(scenario)
                            .type(action.getType())
                            .value(action.getValue())
                            .build();
                })
                .collect(Collectors.toSet());
    }

    private Integer setValue(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        } else {
            throw new IllegalArgumentException("Неподдерживаемый тип значения: " + value.getClass());
        }
    }
}