package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import ru.yandex.practicum.entity.Action;
import ru.yandex.practicum.entity.Condition;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.entity.ScenarioActionId;
import ru.yandex.practicum.entity.ScenarioConditionId;
import ru.yandex.practicum.entity.Sensor;

import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioActionRepository;
import ru.yandex.practicum.repository.ScenarioConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final DecoderFactory decoderFactory = DecoderFactory.get();

    @Transactional
    public void processHubEvent(byte[] eventData) {
        try {
            HubEventAvro hubEvent = deserializeHubEvent(eventData);
            String hubId = hubEvent.getHubId();
            log.info("Processing hub event for hub: {}, payload type: {}",
                    hubId, hubEvent.getPayload().getClass().getSimpleName());

            if (hubEvent.getPayload() instanceof DeviceAddedEventAvro) {
                processDeviceAddedEvent(hubId, (DeviceAddedEventAvro) hubEvent.getPayload());
            } else if (hubEvent.getPayload() instanceof DeviceRemovedEventAvro) {
                processDeviceRemovedEvent(hubId, (DeviceRemovedEventAvro) hubEvent.getPayload());
            } else if (hubEvent.getPayload() instanceof ScenarioAddedEventAvro) {
                processScenarioAddedEvent(hubId, (ScenarioAddedEventAvro) hubEvent.getPayload());
            } else if (hubEvent.getPayload() instanceof ScenarioRemovedEventAvro) {
                processScenarioRemovedEvent(hubId, (ScenarioRemovedEventAvro) hubEvent.getPayload());
            } else {
                log.warn("Unknown hub event payload type: {}", hubEvent.getPayload().getClass().getSimpleName());
            }

            log.info("Successfully processed hub event for hub: {}", hubId);
        } catch (Exception e) {
            log.error("Failed to process hub event", e);
        }
    }

    private void processDeviceAddedEvent(String hubId, DeviceAddedEventAvro event) {
        try {
            if (sensorRepository.existsById(event.getId())) {
                log.debug("Sensor {} already exists for hub: {}, skipping", event.getId(), hubId);
                return;
            }

            Sensor sensor = new Sensor();
            sensor.setId(event.getId());
            sensor.setHubId(hubId);
            sensorRepository.save(sensor);

            log.info("Successfully added sensor: {} for hub: {}, type: {}",
                    event.getId(), hubId, event.getType());
        } catch (Exception e) {
            log.error("Failed to add sensor: {} for hub: {}", event.getId(), hubId, e);
        }
    }

    private void processDeviceRemovedEvent(String hubId, DeviceRemovedEventAvro event) {
        try {
            if (!sensorRepository.existsById(event.getId())) {
                log.debug("Sensor {} not found for hub: {}, skipping removal", event.getId(), hubId);
                return;
            }

            sensorRepository.deleteById(event.getId());

            log.info("Successfully removed sensor: {} from hub: {}", event.getId(), hubId);
        } catch (Exception e) {
            log.error("Failed to remove sensor: {} from hub: {}", event.getId(), hubId, e);
        }
    }

    @Transactional
    private void processScenarioAddedEvent(String hubId, ScenarioAddedEventAvro event) {
        try {
            Optional<Scenario> existingScenario = scenarioRepository.findByHubIdAndName(hubId, event.getName());

            Scenario scenario;
            if (existingScenario.isPresent()) {
                scenario = existingScenario.get();
                scenarioConditionRepository.deleteByIdScenarioId(scenario.getId());
                scenarioActionRepository.deleteByIdScenarioId(scenario.getId());
                scenarioRepository.flush();
            } else {
                scenario = new Scenario();
                scenario.setHubId(hubId);
                scenario.setName(event.getName());
                scenario = scenarioRepository.save(scenario);
            }

            log.info("Processing scenario: {} for hub: {} with {} conditions and {} actions",
                    event.getName(), hubId, event.getConditions().size(), event.getActions().size());

            for (ScenarioConditionAvro condition : event.getConditions()) {
                try {
                    saveScenarioCondition(scenario, condition);
                } catch (Exception e) {
                    log.error("Failed to save condition for scenario: {}, sensor: {}",
                            event.getName(), condition.getSensorId(), e);
                }
            }

            for (DeviceActionAvro action : event.getActions()) {
                try {
                    saveScenarioAction(scenario, action);
                } catch (Exception e) {
                    log.error("Failed to save action for scenario: {}, sensor: {}",
                            event.getName(), action.getSensorId(), e);
                }
            }

            log.info("Successfully processed scenario: {} for hub: {}", event.getName(), hubId);

        } catch (DataIntegrityViolationException e) {
            log.error("Data integrity violation while processing scenario: {} for hub: {}",
                    event.getName(), hubId, e);
            throw e;
        } catch (Exception e) {
            log.error("Failed to process scenario: {} for hub: {}", event.getName(), hubId, e);
            throw e;
        }
    }

    private void saveScenarioCondition(Scenario scenario, ScenarioConditionAvro condition) {
        try {
            String sensorId = condition.getSensorId();
            Sensor sensor = sensorRepository.findById(sensorId)
                    .orElseThrow(() -> new RuntimeException("Sensor not found: " + sensorId + " for scenario: " + scenario.getName()));

            Condition conditionEntity = new Condition();
            conditionEntity.setType(condition.getType().toString());
            conditionEntity.setOperation(condition.getOperation().toString());

            Integer conditionValue = extractConditionValue(condition);
            conditionEntity.setValue(conditionValue);

            Condition savedCondition = conditionRepository.save(conditionEntity);

            ScenarioConditionId id = new ScenarioConditionId(
                    scenario.getId(),
                    sensorId,
                    savedCondition.getId()
            );

            ru.yandex.practicum.entity.ScenarioCondition scenarioCondition = new ru.yandex.practicum.entity.ScenarioCondition();
            scenarioCondition.setId(id);
            scenarioCondition.setScenario(scenario);
            scenarioCondition.setSensor(sensor);
            scenarioCondition.setCondition(savedCondition);

            scenarioConditionRepository.save(scenarioCondition);

            log.debug("Saved condition for scenario: {}, sensor: {}, type: {}, operation: {}, value: {}",
                    scenario.getName(), sensorId, condition.getType(), condition.getOperation(), conditionValue);
        } catch (Exception e) {
            log.error("Failed to save condition for scenario: {}, sensor: {}",
                    scenario.getName(), condition.getSensorId(), e);
            throw e;
        }
    }

    private void saveScenarioAction(Scenario scenario, DeviceActionAvro action) {
        try {
            String sensorId = action.getSensorId();
            Sensor sensor = sensorRepository.findById(sensorId)
                    .orElseThrow(() -> new RuntimeException("Sensor not found: " + sensorId + " for scenario: " + scenario.getName()));

            Action actionEntity = new Action();
            actionEntity.setType(action.getType().toString());
            actionEntity.setValue(action.getValue());

            Action savedAction = actionRepository.save(actionEntity);

            ScenarioActionId id = new ScenarioActionId(
                    scenario.getId(),
                    sensorId,
                    savedAction.getId()
            );

            ru.yandex.practicum.entity.ScenarioAction scenarioAction = new ru.yandex.practicum.entity.ScenarioAction();
            scenarioAction.setId(id);
            scenarioAction.setScenario(scenario);
            scenarioAction.setSensor(sensor);
            scenarioAction.setAction(savedAction);

            scenarioActionRepository.save(scenarioAction);

            log.debug("Saved action for scenario: {}, sensor: {}, type: {}, value: {}",
                    scenario.getName(), sensorId, action.getType(), action.getValue());
        } catch (Exception e) {
            log.error("Failed to save action for scenario: {}, sensor: {}",
                    scenario.getName(), action.getSensorId(), e);
            throw e;
        }
    }

    private Integer extractConditionValue(ScenarioConditionAvro condition) {
        Object value = condition.getValue();

        if (value == null) {
            return null;
        }

        if (value instanceof Integer) {
            return (Integer) value;
        }

        if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        }

        log.warn("Unknown value type: {} for condition", value.getClass().getSimpleName());
        return null;
    }

    private void processScenarioRemovedEvent(String hubId, ScenarioRemovedEventAvro event) {
        try {
            scenarioRepository.findByHubIdAndName(hubId, event.getName())
                    .ifPresent(scenario -> {
                        scenarioConditionRepository.deleteByIdScenarioId(scenario.getId());
                        scenarioActionRepository.deleteByIdScenarioId(scenario.getId());
                        scenarioRepository.delete(scenario);
                        log.info("Successfully removed scenario: {} from hub: {}", event.getName(), hubId);
                    });
        } catch (Exception e) {
            log.error("Failed to remove scenario: {} from hub: {}", event.getName(), hubId, e);
        }
    }

    private HubEventAvro deserializeHubEvent(byte[] data) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
            BinaryDecoder decoder = decoderFactory.binaryDecoder(inputStream, null);
            SpecificDatumReader<HubEventAvro> reader = new SpecificDatumReader<>(HubEventAvro.getClassSchema());
            return reader.read(null, decoder);
        }
    }
}