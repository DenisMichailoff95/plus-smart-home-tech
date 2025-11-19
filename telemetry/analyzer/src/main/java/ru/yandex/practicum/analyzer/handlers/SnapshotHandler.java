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

        scenarios.stream()
                .filter(scenario -> handleScenario(scenario, sensorStateMap))
                .forEach(scenario -> {
                    log.info("Сценарий '{}' выполняется, отправка действий", scenario.getName());
                    sendScenarioActions(scenario);
                });
    }

    private boolean handleScenario(Scenario scenario, Map<String, SensorStateAvro> sensorStateMap) {
        List<Condition> conditions = conditionRepository.findAllByScenario(scenario);
        log.info("Проверка сценария '{}'. Условий: {}", scenario.getName(), conditions.size());

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
            boolean result = false;
            switch (condition.getType()) {
                case LUMINOSITY -> {
                    LightSensorAvro lightSensor = (LightSensorAvro) sensorState.getData();
                    result = handleOperation(condition, lightSensor.getLuminosity());
                    log.debug("Условие LUMINOSITY: датчик={}, текущее={}, целевое={}, операция={}, результат={}",
                            sensorId, lightSensor.getLuminosity(), condition.getValue(),
                            condition.getOperation(), result);
                }
                case TEMPERATURE -> {
                    ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorState.getData();
                    result = handleOperation(condition, climateSensor.getTemperatureC());
                    log.debug("Условие TEMPERATURE: датчик={}, текущее={}, целевое={}, операция={}, результат={}",
                            sensorId, climateSensor.getTemperatureC(), condition.getValue(),
                            condition.getOperation(), result);
                }
                case MOTION -> {
                    MotionSensorAvro motionSensor = (MotionSensorAvro) sensorState.getData();
                    int motionValue = motionSensor.getMotion() ? 1 : 0;
                    result = handleOperation(condition, motionValue);
                    log.debug("Условие MOTION: датчик={}, текущее={}, целевое={}, операция={}, результат={}",
                            sensorId, motionSensor.getMotion(), condition.getValue(),
                            condition.getOperation(), result);
                }
                case SWITCH -> {
                    SwitchSensorAvro switchSensor = (SwitchSensorAvro) sensorState.getData();
                    int switchValue = switchSensor.getState() ? 1 : 0;
                    result = handleOperation(condition, switchValue);
                    log.debug("Условие SWITCH: датчик={}, текущее={}, целевое={}, операция={}, результат={}",
                            sensorId, switchSensor.getState(), condition.getValue(),
                            condition.getOperation(), result);
                }
                case CO2LEVEL -> {
                    ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorState.getData();
                    result = handleOperation(condition, climateSensor.getCo2Level());
                    log.debug("Условие CO2LEVEL: датчик={}, текущее={}, целевое={}, операция={}, результат={}",
                            sensorId, climateSensor.getCo2Level(), condition.getValue(),
                            condition.getOperation(), result);
                }
                case HUMIDITY -> {
                    ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorState.getData();
                    result = handleOperation(condition, climateSensor.getHumidity());
                    log.debug("Условие HUMIDITY: датчик={}, текущее={}, целевое={}, операция={}, результат={}",
                            sensorId, climateSensor.getHumidity(), condition.getValue(),
                            condition.getOperation(), result);
                }
                default -> {
                    log.warn("Неизвестный тип условия: {}", condition.getType());
                    return false;
                }
            }
            return result;
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
            case EQUALS -> {
                return targetValue.equals(currentValue);
            }
            case LOWER_THAN -> {
                return currentValue < targetValue;
            }
            case GREATER_THAN -> {
                return currentValue > targetValue;
            }
            default -> {
                log.warn("Неизвестная операция: {}", conditionOperation);
                return false;
            }
        }
    }

    private void sendScenarioActions(Scenario scenario) {
        List<ru.yandex.practicum.analyzer.model.Action> actions = actionRepository.findAllByScenario(scenario);
        log.info("Отправка {} действий для сценария '{}'", actions.size(), scenario.getName());
        actions.forEach(action -> {
            try {
                hubRouterClient.sendAction(action);
                log.info("Действие отправлено: сценарий={}, датчик={}, тип={}, значение={}",
                        scenario.getName(), action.getSensor().getId(),
                        action.getType(), action.getValue());
            } catch (Exception e) {
                log.error("Ошибка при отправке действия для сценария '{}'", scenario.getName(), e);
            }
        });
    }
}