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

        if (!sensorStates.containsKey(sensorId)) {
            log.debug("Sensor {} not found in snapshot for hub {}", sensorId, snapshot.getHubId());
            return false;
        }

        SensorStateAvro sensorState = sensorStates.get(sensorId);
        Object sensorData = sensorState.getData();
        String conditionType = scenarioCondition.getType();
        String operation = scenarioCondition.getOperation();
        Integer conditionValue = scenarioCondition.getValue();

        log.trace("Evaluating condition: sensor={}, type={}, operation={}, value={}",
                sensorId, conditionType, operation, conditionValue);

        return evaluateSensorData(sensorData, conditionType, operation, conditionValue);
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

            boolean result = performOperation(sensorValue, operation, conditionValue);
            log.trace("Condition evaluation: {} {} {} = {}", sensorValue, operation, conditionValue, result);
            return result;

        } catch (Exception e) {
            log.error("Error evaluating sensor data for condition type: {}", conditionType, e);
            return false;
        }
    }

    private Integer extractSensorValue(Object sensorData, String conditionType) {
        if (sensorData == null) {
            return null;
        }

        try {
            switch (conditionType.toUpperCase()) {
                case "TEMPERATURE":
                    if (sensorData instanceof ClimateSensorAvro) {
                        return (int) ((ClimateSensorAvro) sensorData).getTemperatureC();
                    } else if (sensorData instanceof TemperatureSensorAvro) {
                        return (int) ((TemperatureSensorAvro) sensorData).getTemperatureC();
                    }
                    break;

                case "HUMIDITY":
                    if (sensorData instanceof ClimateSensorAvro) {
                        return (int) ((ClimateSensorAvro) sensorData).getHumidity();
                    }
                    break;

                case "CO2LEVEL":
                    if (sensorData instanceof ClimateSensorAvro) {
                        return (int) ((ClimateSensorAvro) sensorData).getCo2Level();
                    }
                    break;

                case "MOTION":
                    if (sensorData instanceof MotionSensorAvro) {
                        return ((MotionSensorAvro) sensorData).getMotion() ? 1 : 0;
                    }
                    break;

                case "LUMINOSITY":
                    if (sensorData instanceof LightSensorAvro) {
                        return (int) ((LightSensorAvro) sensorData).getLuminosity();
                    }
                    break;

                case "SWITCH":
                    if (sensorData instanceof SwitchSensorAvro) {
                        return ((SwitchSensorAvro) sensorData).getState() ? 1 : 0;
                    }
                    break;

                default:
                    log.warn("Unknown condition type: {}", conditionType);
            }
        } catch (Exception e) {
            log.error("Error extracting sensor value for type: {}", conditionType, e);
        }

        return null;
    }

    private boolean performOperation(Integer sensorValue, String operation, Integer conditionValue) {
        switch (operation.toUpperCase()) {
            case "EQUALS":
                return sensorValue.equals(conditionValue);
            case "GREATER_THAN":
                return sensorValue > conditionValue;
            case "LOWER_THAN":
                return sensorValue < conditionValue;
            default:
                log.warn("Unknown operation: {}", operation);
                return false;
        }
    }

    public boolean evaluateAllConditions(SensorsSnapshotAvro snapshot,
                                         List<ScenarioCondition> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            log.debug("No conditions to evaluate");
            return false;
        }

        boolean allConditionsMet = conditions.stream().allMatch(condition ->
                evaluateCondition(snapshot, condition)
        );

        log.debug("All conditions met: {} for {} conditions", allConditionsMet, conditions.size());
        return allConditionsMet;
    }
}