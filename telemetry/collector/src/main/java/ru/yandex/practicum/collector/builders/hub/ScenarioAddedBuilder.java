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
                        .setConditions(mapConditionsToAvro(event.getConditions()))
                        .setActions(mapActionsToAvro(event.getActions()))
                        .build())
                .build();
    }

    private List<ScenarioConditionAvro> mapConditionsToAvro(List<ScenarioCondition> conditions) {
        return conditions.stream()
                .map(this::mapConditionToAvro)
                .toList();
    }

    private ScenarioConditionAvro mapConditionToAvro(ScenarioCondition condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(mapConditionType(condition.getType()))
                .setOperation(mapConditionOperation(condition.getOperation()));

        // Критически важная часть: правильная работа с union полем value
        if (condition.getValue() != null) {
            // Для SWITCH и MOTION используем boolean ветку union
            if (condition.getType() == ru.yandex.practicum.collector.enums.ScenarioConditionType.SWITCH ||
                    condition.getType() == ru.yandex.practicum.collector.enums.ScenarioConditionType.MOTION) {
                // Преобразуем: 0 -> false, любое другое число -> true
                boolean boolValue = condition.getValue() != 0;
                builder.setValue(boolValue);
            } else {
                // Для остальных типов используем int ветку union
                builder.setValue(condition.getValue());
            }
        }
        // Если value null, оставляем как есть (используется default null из схемы)

        return builder.build();
    }

    private List<DeviceActionAvro> mapActionsToAvro(List<DeviceAction> actions) {
        return actions.stream()
                .map(this::mapActionToAvro)
                .toList();
    }

    private DeviceActionAvro mapActionToAvro(DeviceAction action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(mapActionType(action.getType()));

        // Для DeviceActionAvro.value используется union{null, int}
        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        }
        // Если value null, оставляем как есть

        return builder.build();
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