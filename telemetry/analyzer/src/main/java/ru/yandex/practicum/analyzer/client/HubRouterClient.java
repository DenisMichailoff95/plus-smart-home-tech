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

    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public HubRouterClient(@GrpcClient("hub-router")
                           HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void sendAction(Action action) {
        try {
            DeviceActionRequest deviceActionRequest = buildActionRequest(action);
            log.info("Отправка действия в Hub Router: hubId={}, scenario={}, sensorId={}, type={}, value={}",
                    deviceActionRequest.getHubId(),
                    deviceActionRequest.getScenarioName(),
                    deviceActionRequest.getAction().getSensorId(),
                    deviceActionRequest.getAction().getType(),
                    deviceActionRequest.getAction().getValue());

            hubRouterClient.handleDeviceAction(deviceActionRequest);
            log.info("Действие успешно отправлено в hub-router");
        } catch (Exception e) {
            log.error("Ошибка при отправке действия в Hub Router", e);
            throw new RuntimeException("Failed to send action to Hub Router", e);
        }
    }

    private DeviceActionRequest buildActionRequest(Action action) {
        return DeviceActionRequest.newBuilder()
                .setHubId(action.getScenario().getHubId())
                .setScenarioName(action.getScenario().getName())
                .setAction(DeviceActionProto.newBuilder()
                        .setSensorId(action.getSensor().getId())
                        .setType(actionTypeProto(action.getType()))
                        .setValue(action.getValue() != null ? action.getValue() : 0)
                        .build())
                .setTimestamp(setTimestamp())
                .build();
    }

    private ActionTypeProto actionTypeProto(ActionTypeAvro actionType) {
        return switch (actionType) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
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