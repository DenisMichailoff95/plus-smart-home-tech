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

        log.info("Обработка снапшота для хаба {}. Найдено сценариев: {}, Датчиков в снапшоте: {}",
                sensorsSnapshot.getHubId(), scenarios.size(), sensorStateMap.size());

        for (Scenario scenario : scenarios) {
            log.debug("Проверка сценария: '{}' для хаба {}", scenario.getName(), scenario.getHubId());
            boolean shouldExecute = handleScenario(scenario, sensorStateMap);
            if (shouldExecute) {
                log.info("Сценарий '{}' выполняется, отправка действий", scenario.getName());
                sendScenarioActions(scenario);
            } else {
                log.debug("Сценарий '{}' не выполняется - условия не выполнены", scenario.getName());
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
                    if (sensorState.getData() instanceof LightSensorAvro) {
                        LightSensorAvro lightSensor = (LightSensorAvro) sensorState.getData();
                        return handleOperation(condition, lightSensor.getLuminosity());
                    }
                }
                case TEMPERATURE -> {
                    // Проверяем оба типа сенсоров для температуры
                    if (sensorState.getData() instanceof ClimateSensorAvro) {
                        ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorState.getData();
                        return handleOperation(condition, climateSensor.getTemperatureC());
                    } else if (sensorState.getData() instanceof TemperatureSensorAvro) {
                        TemperatureSensorAvro tempSensor = (TemperatureSensorAvro) sensorState.getData();
                        return handleOperation(condition, tempSensor.getTemperatureC());
                    }
                }
                case MOTION -> {
                    if (sensorState.getData() instanceof MotionSensorAvro) {
                        MotionSensorAvro motionSensor = (MotionSensorAvro) sensorState.getData();
                        // Для boolean условий используем специальную обработку
                        return handleBooleanOperation(condition, motionSensor.getMotion());
                    }
                }
                case SWITCH -> {
                    if (sensorState.getData() instanceof SwitchSensorAvro) {
                        SwitchSensorAvro switchSensor = (SwitchSensorAvro) sensorState.getData();
                        return handleBooleanOperation(condition, switchSensor.getState());
                    }
                }
                case CO2LEVEL -> {
                    if (sensorState.getData() instanceof ClimateSensorAvro) {
                        ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorState.getData();
                        return handleOperation(condition, climateSensor.getCo2Level());
                    }
                }
                case HUMIDITY -> {
                    if (sensorState.getData() instanceof ClimateSensorAvro) {
                        ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorState.getData();
                        return handleOperation(condition, climateSensor.getHumidity());
                    }
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

        log.warn("Несоответствие типа данных для условия: тип условия={}, тип данных={}",
                condition.getType(), sensorState.getData().getClass().getSimpleName());
        return false;
    }

    private boolean handleBooleanOperation(Condition condition, Boolean currentValue) {
        if (currentValue == null) {
            log.warn("Текущее boolean значение равно null для условия {}", condition.getId());
            return false;
        }

        Integer targetValue = condition.getValue();
        if (targetValue == null) {
            log.warn("Целевое значение равно null для условия {}", condition.getId());
            return false;
        }

        // Конвертируем boolean в int (true=1, false=0)
        int currentIntValue = currentValue ? 1 : 0;

        return handleOperation(condition, currentIntValue);
    }

    private boolean handleOperation(Condition condition, Integer currentValue) {
        if (currentValue == null) {
            log.warn("Текущее значение равно null для условия {}", condition.getId());
            return false;
        }

        ConditionOperationAvro conditionOperation = condition.getOperation();
        Integer targetValue = condition.getValue();

        if (targetValue == null) {
            log.warn("Целевое значение равно null для условия {}", condition.getId());
            return false;
        }

        boolean result;
        switch (conditionOperation) {
            case EQUALS:
                result = targetValue.equals(currentValue);
                break;
            case LOWER_THAN:
                result = currentValue < targetValue;
                break;
            case GREATER_THAN:
                result = currentValue > targetValue;
                break;
            default:
                log.warn("Неизвестная операция: {}", conditionOperation);
                return false;
        }

        log.debug("Проверка условия: текущее={}, целевое={}, операция={}, результат={}",
                currentValue, targetValue, conditionOperation, result);
        return result;
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