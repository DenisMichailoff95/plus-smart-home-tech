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
        log.info("Состояния датчиков в снапшоте: {}", sensorStateMap.keySet());

        for (Scenario scenario : scenarios) {
            boolean shouldExecute = handleScenario(scenario, sensorStateMap);
            if (shouldExecute) {
                log.info("Сценарий '{}' выполняется, отправка действий", scenario.getName());
                sendScenarioActions(scenario);
            } else {
                log.info("Сценарий '{}' не выполняется", scenario.getName());
            }
        }
    }

    private boolean handleScenario(Scenario scenario, Map<String, SensorStateAvro> sensorStateMap) {
        List<Condition> conditions = conditionRepository.findAllByScenario(scenario);
        log.info("Проверка сценария '{}'. Условий: {}", scenario.getName(), conditions.size());

        if (conditions.isEmpty()) {
            log.warn("Сценарий '{}' не имеет условий", scenario.getName());
            return false;
        }

        boolean allConditionsMet = true;
        for (Condition condition : conditions) {
            boolean conditionMet = checkCondition(condition, sensorStateMap);
            log.info("Условие: датчик={}, тип={}, операция={}, целевое={}, выполнено={}",
                    condition.getSensor().getId(), condition.getType(),
                    condition.getOperation(), condition.getValue(), conditionMet);

            if (!conditionMet) {
                allConditionsMet = false;
                // Не прерываем цикл для логирования всех условий
            }
        }

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
            Object sensorData = sensorState.getData();
            if (sensorData == null) {
                log.warn("Данные датчика {} равны null", sensorId);
                return false;
            }

            Integer currentValue = extractSensorValue(sensorData, condition.getType());
            if (currentValue == null) {
                log.warn("Не удалось извлечь значение для датчика {} типа {}", sensorId, condition.getType());
                return false;
            }

            boolean result = handleOperation(condition, currentValue);
            log.debug("Условие {}: датчик={}, текущее={}, целевое={}, операция={}, результат={}",
                    condition.getType(), sensorId, currentValue, condition.getValue(),
                    condition.getOperation(), result);
            return result;

        } catch (Exception e) {
            log.error("Ошибка при проверке условия для датчика {}", sensorId, e);
            return false;
        }
    }

    private Integer extractSensorValue(Object sensorData, ConditionTypeAvro conditionType) {
        try {
            switch (conditionType) {
                case LUMINOSITY:
                    LightSensorAvro lightSensor = (LightSensorAvro) sensorData;
                    return lightSensor.getLuminosity();
                case TEMPERATURE:
                    ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorData;
                    return climateSensor.getTemperatureC();
                case MOTION:
                    MotionSensorAvro motionSensor = (MotionSensorAvro) sensorData;
                    return motionSensor.getMotion() ? 1 : 0;
                case SWITCH:
                    SwitchSensorAvro switchSensor = (SwitchSensorAvro) sensorData;
                    return switchSensor.getState() ? 1 : 0;
                case CO2LEVEL:
                    ClimateSensorAvro co2Sensor = (ClimateSensorAvro) sensorData;
                    return co2Sensor.getCo2Level();
                case HUMIDITY:
                    ClimateSensorAvro humiditySensor = (ClimateSensorAvro) sensorData;
                    return humiditySensor.getHumidity();
                default:
                    log.warn("Неизвестный тип условия: {}", conditionType);
                    return null;
            }
        } catch (ClassCastException e) {
            log.error("Ошибка приведения типа для условия {}", conditionType, e);
            return null;
        }
    }

    private boolean handleOperation(Condition condition, Integer currentValue) {
        if (currentValue == null) {
            return false;
        }

        ConditionOperationAvro conditionOperation = condition.getOperation();
        Integer targetValue = condition.getValue();

        if (targetValue == null) {
            log.warn("Целевое значение условия равно null");
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
                log.info("Действие успешно отправлено");
            } catch (Exception e) {
                log.error("Ошибка при отправке действия для сценария '{}'", scenario.getName(), e);
            }
        }
    }
}