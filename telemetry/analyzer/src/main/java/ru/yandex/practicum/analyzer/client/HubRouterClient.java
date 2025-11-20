package ru.yandex.practicum.analyzer.client;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.analyzer.model.Action;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;

import java.time.Instant;

@Slf4j
@Service
public class HubRouterClient {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterStub;

    public void sendAction(Action action) {
        try {
            log.debug("Attempting to send action - scenario: {}, sensor: {}, type: {}, value: {}",
                    action.getScenario().getName(),
                    action.getSensor().getId(),
                    action.getType(),
                    action.getValue());

            DeviceActionRequest deviceActionRequest = buildActionRequest(action);

            log.info("Sending action to Hub Router - hubId: {}, scenario: {}, sensorId: {}, type: {}, value: {}",
                    deviceActionRequest.getHubId(),
                    deviceActionRequest.getScenarioName(),
                    deviceActionRequest.getAction().getSensorId(),
                    deviceActionRequest.getAction().getType(),
                    deviceActionRequest.getAction().getValue());

            hubRouterStub.handleDeviceAction(deviceActionRequest);
            log.info("Action successfully sent to hub-router for scenario: {}", action.getScenario().getName());
        } catch (Exception e) {
            log.error("Error sending action to Hub Router for scenario '{}'",
                    action.getScenario().getName(), e);
            throw new RuntimeException("Failed to send action to Hub Router", e);
        }
    }

    private DeviceActionRequest buildActionRequest(Action action) {
        DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                .setSensorId(action.getSensor().getId())
                .setType(actionTypeProto(action.getType()));

        if (action.getValue() != null) {
            actionBuilder.setValue(action.getValue());
        } else {
            actionBuilder.setValue(0);
        }

        return DeviceActionRequest.newBuilder()
                .setHubId(action.getScenario().getHubId())
                .setScenarioName(action.getScenario().getName())
                .setAction(actionBuilder.build())
                .setTimestamp(setTimestamp())
                .build();
    }

    private ActionTypeProto actionTypeProto(ActionTypeAvro actionType) {
        return switch (actionType) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
            default -> ActionTypeProto.ACTIVATE;
        };
    }

    private Timestamp setTimestamp() {
        Instant instant = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}