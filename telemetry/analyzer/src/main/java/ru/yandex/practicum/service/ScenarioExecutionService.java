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
        log.debug("Analyzing snapshot for hub: {}, scenarios: {}", hubId, scenarios.size());

        for (Scenario scenario : scenarios) {
            try {
                log.debug("Checking scenario: {} for hub: {}", scenario.getName(), hubId);

                List<ScenarioCondition> conditions = scenarioConditionRepository
                        .findWithAssociationsByScenarioId(scenario.getId());

                if (conditions.isEmpty()) {
                    log.debug("No conditions found for scenario: {}", scenario.getName());
                    continue;
                }

                boolean conditionsMet = conditionEvaluationService.evaluateAllConditions(snapshot, conditions);
                log.debug("Conditions met for scenario {}: {}", scenario.getName(), conditionsMet);

                if (conditionsMet) {
                    log.info("Executing scenario: {} for hub: {}", scenario.getName(), hubId);
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
                    .findWithAssociationsByScenarioId(scenario.getId());

            if (scenarioActions.isEmpty()) {
                log.warn("No actions found for scenario: {}", scenario.getName());
                return;
            }

            log.debug("Executing {} actions for scenario: {} on hub: {}",
                    scenarioActions.size(), scenario.getName(), hubId);

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

            log.debug("Sending action to Hub Router - Hub: {}, Scenario: {}, Sensor: {}, Action: {}",
                    hubId, scenarioName, sensor.getId(), action.getType());

            DeviceActionProto actionProto = buildDeviceActionProto(sensor, action);
            DeviceActionRequest request = buildDeviceActionRequest(hubId, scenarioName, actionProto);

            hubRouterClient.handleDeviceAction(request);
            log.debug("Successfully sent device action for scenario: {}, sensor: {}",
                    scenarioName, sensor.getId());

        } catch (Exception e) {
            log.error("Failed to send device action for scenario: {}, sensor: {}",
                    scenarioName, scenarioAction.getSensor().getId(), e);
        }
    }

    private DeviceActionProto buildDeviceActionProto(Sensor sensor, Action action) {
        return DeviceActionProto.newBuilder()
                .setSensorId(sensor.getId())
                .setType(mapActionType(action.getType()))
                .setValue(action.getValue() != null ? action.getValue() : 0)
                .build();
    }

    private DeviceActionRequest buildDeviceActionRequest(String hubId, String scenarioName, DeviceActionProto actionProto) {
        Instant now = Instant.now();
        return DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(actionProto)
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .build();
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