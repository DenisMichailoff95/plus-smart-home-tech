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
        String hubId = sensorsSnapshot.getHubId();
        Map<String, SensorStateAvro> sensorStateMap = sensorsSnapshot.getSensorsState();
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);

        log.info("Processing snapshot for hub: {}. Scenarios: {}, Sensors: {}",
                hubId, scenarios.size(), sensorStateMap.size());

        if (scenarios.isEmpty()) {
            log.debug("No scenarios found for hub: {}", hubId);
            return;
        }

        for (Scenario scenario : scenarios) {
            log.debug("Checking scenario: '{}' for hub: {}", scenario.getName(), hubId);
            boolean shouldExecute = handleScenario(scenario, sensorStateMap);

            if (shouldExecute) {
                log.info("Scenario '{}' conditions met, executing actions", scenario.getName());
                sendScenarioActions(scenario);
            } else {
                log.debug("Scenario '{}' conditions not met", scenario.getName());
            }
        }
    }

    private boolean handleScenario(Scenario scenario, Map<String, SensorStateAvro> sensorStateMap) {
        List<Condition> conditions = conditionRepository.findAllByScenario(scenario);

        if (conditions.isEmpty()) {
            log.warn("Scenario '{}' has no conditions", scenario.getName());
            return false;
        }

        log.debug("Checking {} conditions for scenario '{}'", conditions.size(), scenario.getName());

        boolean allConditionsMet = true;

        for (Condition condition : conditions) {
            boolean conditionMet = checkCondition(condition, sensorStateMap);
            log.debug("Condition check - sensor: {}, type: {}, met: {}",
                    condition.getSensor().getId(), condition.getType(), conditionMet);

            if (!conditionMet) {
                allConditionsMet = false;
                break; // No need to check further if one condition fails
            }
        }

        log.info("Scenario '{}' all conditions met: {}", scenario.getName(), allConditionsMet);
        return allConditionsMet;
    }

    private boolean checkCondition(Condition condition, Map<String, SensorStateAvro> sensorStateMap) {
        String sensorId = condition.getSensor().getId();
        SensorStateAvro sensorState = sensorStateMap.get(sensorId);

        if (sensorState == null) {
            log.warn("Sensor {} not found in snapshot", sensorId);
            return false;
        }

        Object sensorData = sensorState.getData();
        Integer conditionValue = condition.getValue();

        try {
            switch (condition.getType()) {
                case LUMINOSITY:
                    if (sensorData instanceof LightSensorAvro) {
                        LightSensorAvro lightSensor = (LightSensorAvro) sensorData;
                        return compareValues(lightSensor.getLuminosity(), conditionValue, condition.getOperation());
                    }
                    break;

                case TEMPERATURE:
                    if (sensorData instanceof ClimateSensorAvro) {
                        ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorData;
                        return compareValues(climateSensor.getTemperatureC(), conditionValue, condition.getOperation());
                    } else if (sensorData instanceof TemperatureSensorAvro) {
                        TemperatureSensorAvro tempSensor = (TemperatureSensorAvro) sensorData;
                        return compareValues(tempSensor.getTemperatureC(), conditionValue, condition.getOperation());
                    }
                    break;

                case MOTION:
                    if (sensorData instanceof MotionSensorAvro) {
                        MotionSensorAvro motionSensor = (MotionSensorAvro) sensorData;
                        int motionValue = motionSensor.getMotion() ? 1 : 0;
                        return compareValues(motionValue, conditionValue, condition.getOperation());
                    }
                    break;

                case SWITCH:
                    if (sensorData instanceof SwitchSensorAvro) {
                        SwitchSensorAvro switchSensor = (SwitchSensorAvro) sensorData;
                        int switchValue = switchSensor.getState() ? 1 : 0;
                        return compareValues(switchValue, conditionValue, condition.getOperation());
                    }
                    break;

                case CO2LEVEL:
                    if (sensorData instanceof ClimateSensorAvro) {
                        ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorData;
                        return compareValues(climateSensor.getCo2Level(), conditionValue, condition.getOperation());
                    }
                    break;

                case HUMIDITY:
                    if (sensorData instanceof ClimateSensorAvro) {
                        ClimateSensorAvro climateSensor = (ClimateSensorAvro) sensorData;
                        return compareValues(climateSensor.getHumidity(), conditionValue, condition.getOperation());
                    }
                    break;

                default:
                    log.warn("Unknown condition type: {}", condition.getType());
                    return false;
            }
        } catch (Exception e) {
            log.error("Error checking condition for sensor: {}", sensorId, e);
            return false;
        }

        log.warn("Data type mismatch for condition: sensor={}, conditionType={}, dataType={}",
                sensorId, condition.getType(), sensorData.getClass().getSimpleName());
        return false;
    }

    private boolean compareValues(int sensorValue, Integer conditionValue, ConditionOperationAvro operation) {
        if (conditionValue == null) {
            log.warn("Condition value is null");
            return false;
        }

        switch (operation) {
            case EQUALS:
                return sensorValue == conditionValue;
            case GREATER_THAN:
                return sensorValue > conditionValue;
            case LOWER_THAN:
                return sensorValue < conditionValue;
            default:
                log.warn("Unknown operation: {}", operation);
                return false;
        }
    }

    private void sendScenarioActions(Scenario scenario) {
        List<ru.yandex.practicum.analyzer.model.Action> actions = actionRepository.findAllByScenario(scenario);
        log.info("Sending {} actions for scenario '{}'", actions.size(), scenario.getName());

        for (ru.yandex.practicum.analyzer.model.Action action : actions) {
            try {
                log.info("Sending action - scenario: {}, sensor: {}, type: {}, value: {}",
                        scenario.getName(), action.getSensor().getId(), action.getType(), action.getValue());

                hubRouterClient.sendAction(action);
                log.debug("Successfully sent action for scenario: {}", scenario.getName());

            } catch (Exception e) {
                log.error("Failed to send action for scenario: {}, sensor: {}",
                        scenario.getName(), action.getSensor().getId(), e);
            }
        }
    }
}