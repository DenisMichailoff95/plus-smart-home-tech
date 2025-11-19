package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import ru.yandex.practicum.entity.Action;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.entity.ScenarioAction;
import ru.yandex.practicum.entity.ScenarioCondition;
import ru.yandex.practicum.entity.Sensor;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.repository.ScenarioActionRepository;
import ru.yandex.practicum.repository.ScenarioConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class ScenarioExecutionService {

    @GrpcClient("hub-router")
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;
    private final ScenarioRepository scenarioRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final ConditionEvaluationService conditionEvaluationService;

    public void executeScenarios(SensorsSnapshotAvro snapshot, List<Scenario> scenarios) {
        String hubId = snapshot.getHubId();
        log.info("=== ANALYZING SNAPSHOT FOR HUB: {} ===", hubId);
        log.info("Found {} scenarios to check", scenarios.size());

        for (Scenario scenario : scenarios) {
            try {
                log.info("Checking scenario: {} for hub: {}", scenario.getName(), hubId);

                List<ScenarioCondition> conditions = scenarioConditionRepository
                        .findWithAssociationsByIdScenarioId(scenario.getId());
                log.info("Loaded {} conditions for scenario: {}", conditions.size(), scenario.getName());

                if (conditions.isEmpty()) {
                    log.warn("No conditions found for scenario: {}", scenario.getName());
                    continue;
                }

                boolean conditionsMet = conditionEvaluationService.evaluateAllConditions(snapshot, conditions);
                log.info("Conditions met for scenario {}: {}", scenario.getName(), conditionsMet);

                if (conditionsMet) {
                    log.info("EXECUTING SCENARIO: {} for hub: {}", scenario.getName(), hubId);
                    executeScenarioActions(hubId, scenario);
                }
            } catch (Exception e) {
                log.error("Error executing scenario: {} for hub: {}", scenario.getName(), hubId, e);
            }
        }
    }

    private void executeScenarioActions(String hubId, Scenario scenario) {
        try {
            List<ScenarioAction> scenarioActions = scenarioActionRepository
                    .findWithAssociationsByIdScenarioId(scenario.getId());
            log.info("Executing {} actions for scenario: {} on hub: {}",
                    scenarioActions.size(), scenario.getName(), hubId);

            if (scenarioActions.isEmpty()) {
                log.warn("No actions found for scenario: {}", scenario.getName());
                return;
            }

            for (ScenarioAction scenarioAction : scenarioActions) {
                executeSingleAction(hubId, scenario.getName(), scenarioAction);
            }

        } catch (Exception e) {
            log.error("Failed to execute scenario actions: {}", scenario.getName(), e);
        }
    }

    private void executeSingleAction(String hubId, String scenarioName, ScenarioAction scenarioAction) {
        try {
            Sensor sensor = scenarioAction.getSensor();
            Action action = scenarioAction.getAction();

            if (sensor == null || action == null) {
                log.warn("Invalid scenario action: missing sensor or action for scenario: {}", scenarioName);
                return;
            }

            log.info("Sending action to Hub Router - Hub: {}, Scenario: {}, Sensor: {}, Action: {}",
                    hubId, scenarioName, sensor.getId(), action.getType());

            DeviceActionProto actionProto = DeviceActionProto.newBuilder()
                    .setSensorId(sensor.getId())
                    .setType(mapActionType(action.getType()))
                    .setValue(action.getValue() != null ? action.getValue() : 0)
                    .build();

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(hubId)
                    .setScenarioName(scenarioName)
                    .setAction(actionProto)
                    .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                            .setSeconds(Instant.now().getEpochSecond())
                            .setNanos(Instant.now().getNano())
                            .build())
                    .build();

            hubRouterClient.handleDeviceAction(request);
            log.info("SUCCESS: Sent device action for scenario: {}, sensor: {}, action: {}",
                    scenarioName, sensor.getId(), action.getType());

        } catch (Exception e) {
            log.error("FAILED to send device action for scenario: {}, sensor: {}",
                    scenarioName, scenarioAction.getSensor().getId(), e);
        }
    }

    private ActionTypeProto mapActionType(String actionType) {
        if (actionType == null) {
            return ActionTypeProto.ACTIVATE;
        }

        switch (actionType.toUpperCase()) {
            case "ACTIVATE":
                return ActionTypeProto.ACTIVATE;
            case "DEACTIVATE":
                return ActionTypeProto.DEACTIVATE;
            case "INVERSE":
                return ActionTypeProto.INVERSE;
            case "SET_VALUE":
                return ActionTypeProto.SET_VALUE;
            default:
                log.warn("Unknown action type: {}, defaulting to ACTIVATE", actionType);
                return ActionTypeProto.ACTIVATE;
        }
    }
}