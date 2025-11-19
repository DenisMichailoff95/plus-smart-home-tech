package ru.yandex.practicum.analyzer.handlers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.client.HubRouterClient;
import ru.yandex.practicum.analyzer.model.Condition;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotHandler {

    private final ConditionRepository conditionRepository;
    private final ScenarioRepository scenarioRepository;
    private final ActionRepository actionRepository;
    private final HubRouterClient hubRouterClient;

    public void buildSnapshot(SensorsSnapshotAvro sensorsSnapshot) {
        Map<String, SensorStateAvro> sensorStateMap = sensorsSnapshot.getSensorsState();
        List<Scenario> scenarios = scenarioRepository.findByHubId(sensorsSnapshot.getHubId());

        log.info("Обработка снапшота для хаба {}. Найдено сценариев: {}",
                sensorsSnapshot.getHubId(), scenarios.size());

        for (Scenario scenario : scenarios) {
            boolean shouldExecute = handleScenario(scenario, sensorStateMap);
            if (shouldExecute) {
                log.info("Сценарий '{}' выполняется, отправка действий", scenario.getName());
                sendScenarioActions(scenario);
            }
        }
    }

    private boolean handleScenario(Scenario scenario, Map<String, SensorStateAvro> sensorStateMap) {
        List<Condition> conditions = conditionRepository.findAllByScenario(scenario);

        if (conditions.isEmpty()) {
            log.warn("Сценарий '{}' не имеет условий", scenario.getName());
            return false;
        }

        boolean allConditionsMet = conditions.stream()
                .allMatch(condition -> checkCondition(condition, sensorStateMap));

        log.info("Сценарий '{}': все условия выполнены - {}", scenario.getName(), allConditionsMet);
        return allConditionsMet;
    }

    private boolean checkCondition(Condition condition, Map<String, SensorStateAvro> sensorStateMap) {
        String sensorId = condition.getSensor().getId();
        SensorStateAvro sensorState = sensorStateMap.get(sensorId);

        if (sensorState == null) {
            log.warn("Датчик {} не найден в снапшоте", sensorId);
            return false;
        }

        try {
            switch (condition.getType()) {
                case LUMINOSITY -> {
                    LightSensorAvro lightSensor = (LightSensorAvro) sensorState.getData();
                    return handleOperation(condition, lightSensor.getLuminosity());
                }
                case TEMPERATURE -> {
                    ClimateSensorAvro temperatureSensor = (ClimateSensorAvro) sensorState.getData();
                    return handleOperation(condition, temperatureSensor.getTemperatureC());
                }
                case MOTION -> {
                    MotionSensorAvro motionSensor = (MotionSensorAvro) sensorState.getData();
                    int motionValue = motionSensor.getMotion() ? 1 : 0;
                    return handleOperation(condition, motionValue);
                }
                case SWITCH -> {
                    SwitchSensorAvro switchSensor = (SwitchSensorAvro) sensorState.getData();
                    int switchValue = switchSensor.getState() ? 1 : 0;
                    return handleOperation(condition, switchValue);
                }
                case CO2LEVEL -> {
                    ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorState.getData();
                    return handleOperation(condition, climateSensor.getCo2Level());
                }
                case HUMIDITY -> {
                    ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorState.getData();
                    return handleOperation(condition, climateSensor.getHumidity());
                }
                default -> {
                    log.warn("Неизвестный тип условия: {}", condition.getType());
                    return false;
                }
            }
        } catch (Exception e) {
            log.error("Ошибка при проверке условия для датчика {}", sensorId, e);
            return false;
        }
    }

    private boolean handleOperation(Condition condition, Integer currentValue) {
        if (currentValue == null) {
            return false;
        }

        ConditionOperationAvro conditionOperation = condition.getOperation();
        Integer targetValue = condition.getValue();

        if (targetValue == null) {
            return false;
        }

        switch (conditionOperation) {
            case EQUALS:
                return targetValue.equals(currentValue);
            case LOWER_THAN:
                return currentValue < targetValue;
            case GREATER_THAN:
                return currentValue > targetValue;
            default:
                log.warn("Неизвестная операция: {}", conditionOperation);
                return false;
        }
    }

    private void sendScenarioActions(Scenario scenario) {
        List<ru.yandex.practicum.analyzer.model.Action> actions = actionRepository.findAllByScenario(scenario);
        log.info("Отправка {} действий для сценария '{}'", actions.size(), scenario.getName());

        for (ru.yandex.practicum.analyzer.model.Action action : actions) {
            try {
                log.info("Отправка действия: сценарий={}, датчик={}, тип={}, значение={}",
                        scenario.getName(), action.getSensor().getId(),
                        action.getType(), action.getValue());
                hubRouterClient.sendAction(action);
            } catch (Exception e) {
                log.error("Ошибка при отправке действия для сценария '{}'", scenario.getName(), e);
            }
        }
    }
}