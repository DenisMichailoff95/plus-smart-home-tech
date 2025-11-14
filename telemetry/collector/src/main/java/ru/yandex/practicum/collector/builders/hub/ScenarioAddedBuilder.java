package ru.yandex.practicum.collector.builders.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.producer.KafkaProducer;
import ru.yandex.practicum.collector.schemas.hubEvent.BaseHubEvent;
import ru.yandex.practicum.collector.schemas.hubEvent.DeviceAction;
import ru.yandex.practicum.collector.schemas.hubEvent.ScenarioAddedEvent;
import ru.yandex.practicum.collector.schemas.hubEvent.ScenarioCondition;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

@Component
public class ScenarioAddedBuilder extends BaseHubBuilder {
    public ScenarioAddedBuilder(KafkaProducer producer) {
        super(producer);
    }

    @Override
    public SpecificRecordBase toAvro(BaseHubEvent hubEvent) {
        ScenarioAddedEvent event = (ScenarioAddedEvent) hubEvent;

        return HubEventAvro.newBuilder()
                .setHubId(hubEvent.getHubId())
                .setTimestamp(hubEvent.getTimestamp())
                .setPayload(ScenarioAddedEventAvro.newBuilder()
                        .setName(event.getName())
                        .setConditions(mapToConditionTypeAvro(event.getConditions()))
                        .setActions(mapToDeviceActionAvro(event.getActions()))
                        .build())
                .build();
    }

    private List<ScenarioConditionAvro> mapToConditionTypeAvro(List<ScenarioCondition> conditions) {
        return conditions.stream()
                .map(c -> {
                    ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                            .setSensorId(c.getSensorId())
                            .setType(mapConditionType(c.getType()))
                            .setOperation(mapConditionOperation(c.getOperation()));

                    // Правильная обработка union поля value
                    if (c.getValue() != null) {
                        // Для boolean условий используем boolean, для остальных - int
                        if (c.getType() == ru.yandex.practicum.collector.enums.ScenarioConditionType.SWITCH ||
                                c.getType() == ru.yandex.practicum.collector.enums.ScenarioConditionType.MOTION) {
                            // Для SWITCH и MOTION используем boolean (true/false)
                            boolean boolValue = c.getValue() != 0;
                            builder.setValue(boolValue);
                        } else {
                            // Для остальных типов используем int
                            builder.setValue(c.getValue());
                        }
                    }
                    // Если value null, оставляем как есть (будет использовано значение по умолчанию null)

                    return builder.build();
                })
                .toList();
    }

    private List<DeviceActionAvro> mapToDeviceActionAvro(List<DeviceAction> deviceActions) {
        return deviceActions.stream()
                .map(da -> {
                    DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                            .setSensorId(da.getSensorId())
                            .setType(mapActionType(da.getType()));

                    // Правильная обработка optional поля value
                    if (da.getValue() != null) {
                        builder.setValue(da.getValue());
                    }
                    // Если value null, оставляем как есть (будет использовано значение по умолчанию null)

                    return builder.build();
                })
                .toList();
    }

    private ConditionTypeAvro mapConditionType(ru.yandex.practicum.collector.enums.ScenarioConditionType type) {
        return switch (type) {
            case MOTION -> ConditionTypeAvro.MOTION;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
        };
    }

    private ConditionOperationAvro mapConditionOperation(ru.yandex.practicum.collector.enums.ScenarioConditionOperationType operation) {
        return switch (operation) {
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
        };
    }

    private ActionTypeAvro mapActionType(ru.yandex.practicum.collector.enums.DeviceActionType type) {
        return switch (type) {
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case INVERSE -> ActionTypeAvro.INVERSE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
        };
    }
}