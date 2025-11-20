package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.entity.ScenarioCondition;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConditionEvaluationService {

    public boolean evaluateCondition(SensorsSnapshotAvro snapshot, ScenarioCondition scenarioCondition) {
        String sensorId = scenarioCondition.getSensor().getId();
        Map<String, SensorStateAvro> sensorStates = snapshot.getSensorsState();

        log.info("Evaluating condition for sensor: {}", sensorId);

        if (!sensorStates.containsKey(sensorId)) {
            log.warn("Sensor {} not found in snapshot for hub {}", sensorId, snapshot.getHubId());
            return false;
        }

        SensorStateAvro sensorState = sensorStates.get(sensorId);
        Object sensorData = sensorState.getData();
        String conditionType = scenarioCondition.getType();
        String operation = scenarioCondition.getOperation();
        Integer conditionValue = scenarioCondition.getValue();

        log.info("Condition details: sensor={}, type={}, operation={}, expectedValue={}",
                sensorId, conditionType, operation, conditionValue);

        boolean result = evaluateSensorData(sensorData, conditionType, operation, conditionValue);
        log.info("Evaluation result for sensor {}: {}", sensorId, result);

        return result;
    }

    private boolean evaluateSensorData(Object sensorData, String conditionType,
                                       String operation, Integer conditionValue) {
        try {
            Integer sensorValue = extractSensorValue(sensorData, conditionType);

            if (sensorValue == null) {
                log.debug("Cannot extract value for condition type: {} from sensor data", conditionType);
                return false;
            }

            if (conditionValue == null) {
                log.debug("Condition value is null for operation: {}", operation);
                return false;
            }

            log.info("Comparing: sensorValue={}, conditionValue={}, operation={}",
                    sensorValue, conditionValue, operation);

            boolean result = performOperation(sensorValue, operation, conditionValue);
            log.info("Comparison result: {}", result);

            return result;

        } catch (Exception e) {
            log.error("Error evaluating sensor data for condition type: {}", conditionType, e);
            return false;
        }
    }

    private Integer extractSensorValue(Object sensorData, String conditionType) {
        if (sensorData == null) {
            log.warn("Sensor data is null for condition type: {}", conditionType);
            return null;
        }

        try {
            Integer value = null;

            switch (conditionType.toUpperCase()) {
                case "TEMPERATURE":
                    if (sensorData instanceof ClimateSensorAvro) {
                        value = (int) ((ClimateSensorAvro) sensorData).getTemperatureC();
                        log.info("Extracted temperature: {}°C", value);
                    } else if (sensorData instanceof TemperatureSensorAvro) {
                        value = (int) ((TemperatureSensorAvro) sensorData).getTemperatureC();
                        log.info("Extracted temperature: {}°C", value);
                    }
                    break;

                case "HUMIDITY":
                    if (sensorData instanceof ClimateSensorAvro) {
                        value = (int) ((ClimateSensorAvro) sensorData).getHumidity();
                        log.info("Extracted humidity: {}%", value);
                    }
                    break;

                case "CO2LEVEL":
                    if (sensorData instanceof ClimateSensorAvro) {
                        value = (int) ((ClimateSensorAvro) sensorData).getCo2Level();
                        log.info("Extracted CO2 level: {}", value);
                    }
                    break;

                case "MOTION":
                    if (sensorData instanceof MotionSensorAvro) {
                        value = ((MotionSensorAvro) sensorData).getMotion() ? 1 : 0;
                        log.info("Extracted motion: {}", value == 1 ? "DETECTED" : "NO MOTION");
                    }
                    break;

                case "LUMINOSITY":
                    if (sensorData instanceof LightSensorAvro) {
                        value = (int) ((LightSensorAvro) sensorData).getLuminosity();
                        log.info("Extracted luminosity: {}", value);
                    }
                    break;

                case "SWITCH":
                    if (sensorData instanceof SwitchSensorAvro) {
                        value = ((SwitchSensorAvro) sensorData).getState() ? 1 : 0;
                        log.info("Extracted switch state: {}", value == 1 ? "ON" : "OFF");
                    }
                    break;

                default:
                    log.warn("Unknown condition type: {}", conditionType);
            }

            if (value == null) {
                log.warn("Could not extract value for condition type: {} from sensor data type: {}",
                        conditionType, sensorData.getClass().getSimpleName());
            }

            return value;

        } catch (Exception e) {
            log.error("Error extracting sensor value for type: {}", conditionType, e);
            return null;
        }
    }

    private boolean performOperation(Integer sensorValue, String operation, Integer conditionValue) {
        boolean result;

        switch (operation.toUpperCase()) {
            case "EQUALS":
                result = sensorValue.equals(conditionValue);
                log.info("EQUALS operation: {} == {} = {}", sensorValue, conditionValue, result);
                break;
            case "GREATER_THAN":
                result = sensorValue > conditionValue;
                log.info("GREATER_THAN operation: {} > {} = {}", sensorValue, conditionValue, result);
                break;
            case "LOWER_THAN":
                result = sensorValue < conditionValue;
                log.info("LOWER_THAN operation: {} < {} = {}", sensorValue, conditionValue, result);
                break;
            default:
                log.warn("Unknown operation: {}", operation);
                result = false;
        }

        return result;
    }

    public boolean evaluateAllConditions(SensorsSnapshotAvro snapshot,
                                         List<ScenarioCondition> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            log.debug("No conditions to evaluate");
            return false;
        }

        log.info("Evaluating {} conditions...", conditions.size());

        boolean allConditionsMet = true;

        for (ScenarioCondition condition : conditions) {
            boolean conditionResult = evaluateCondition(snapshot, condition);
            if (!conditionResult) {
                allConditionsMet = false;
                log.info("Condition NOT met: sensor={}, type={}",
                        condition.getSensor().getId(), condition.getType());
                // Не прерываем цикл, чтобы залогировать все условия
            } else {
                log.info("Condition met: sensor={}, type={}",
                        condition.getSensor().getId(), condition.getType());
            }
        }

        log.info("All conditions met: {} for {} conditions", allConditionsMet, conditions.size());
        return allConditionsMet;
    }
}