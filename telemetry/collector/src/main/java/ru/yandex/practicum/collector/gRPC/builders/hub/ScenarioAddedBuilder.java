package ru.yandex.practicum.collector.gRPC.builders.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.gRPC.producer.KafkaEventProducer;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

@Component
public class ScenarioAddedBuilder extends BaseHubBuilder {
    public ScenarioAddedBuilder(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public HubEventAvro toAvro(HubEventProto hubEvent) {
        ScenarioAddedEventProto scenarioAddedEvent = hubEvent.getScenarioAdded();

        return HubEventAvro.newBuilder()
                .setHubId(hubEvent.getHubId())
                .setTimestamp(mapTimestampToInstant(hubEvent))
                .setPayload(ScenarioAddedEventAvro.newBuilder()
                        .setName(scenarioAddedEvent.getName())
                        .setConditions(mapToConditionTypeAvro(scenarioAddedEvent.getConditionsList()))
                        .setActions(mapToDeviceActionAvro(scenarioAddedEvent.getActionsList()))
                        .build())
                .build();
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    private List<ScenarioConditionAvro> mapToConditionTypeAvro(List<ScenarioConditionProto> conditions) {
        return conditions.stream()
                .map(c -> {
                    ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                            .setSensorId(c.getSensorId())
                            .setType(mapConditionType(c.getType()))
                            .setOperation(mapConditionOperation(c.getOperation()));

                    // Обработка значения в зависимости от типа
                    switch (c.getValueCase()) {
                        case INT_VALUE:
                            builder.setValue(c.getIntValue());
                            break;
                        case BOOL_VALUE:
                            builder.setValue(c.getBoolValue());
                            break;
                        case VALUE_NOT_SET:
                            builder.setValue(null);
                            break;
                    }

                    return builder.build();
                })
                .toList();
    }

    private List<DeviceActionAvro> mapToDeviceActionAvro(List<DeviceActionProto> deviceActions) {
        return deviceActions.stream()
                .map(da -> {
                    DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                            .setSensorId(da.getSensorId())
                            .setType(mapActionType(da.getType()));

                    if (da.hasValue()) {
                        builder.setValue(da.getValue());
                    } else {
                        builder.setValue(null);
                    }

                    return builder.build();
                })
                .toList();
    }

    private ConditionTypeAvro mapConditionType(ru.yandex.practicum.grpc.telemetry.event.ConditionTypeProto type) {
        return switch (type) {
            case MOTION -> ConditionTypeAvro.MOTION;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
            case UNRECOGNIZED -> null;
        };
    }

    private ConditionOperationAvro mapConditionOperation(ru.yandex.practicum.grpc.telemetry.event.ConditionOperationProto operation) {
        return switch (operation) {
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
            case UNRECOGNIZED -> null;
        };
    }

    private ActionTypeAvro mapActionType(ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto type) {
        return switch (type) {
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case INVERSE -> ActionTypeAvro.INVERSE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
            case UNRECOGNIZED -> null;
        };
    }
}