package ru.yandex.practicum.collector.mapper;


import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.ConditionOperationProto;
import ru.yandex.practicum.grpc.telemetry.event.ConditionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceTypeProto;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;

public class Converter {
    public static DeviceTypeAvro mapToDeviceTypeAvro(DeviceTypeProto deviceType) {
        return switch (deviceType) {
            case CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
            case TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Нет такого типа устройств");
        };
    }

    public static ConditionTypeAvro mapToConditionTypeAvro(ConditionTypeProto conditionType) {
        return switch (conditionType) {
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case MOTION -> ConditionTypeAvro.MOTION;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Нет такого типа условия");
        };
    }

    public static ConditionOperationAvro mapToConditionOperationAvro(ConditionOperationProto conditionOperation) {
        return switch (conditionOperation) {
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
            case UNRECOGNIZED ->
                    throw new IllegalArgumentException("Нет такого типа оператора");
        };
    }

    public static ActionTypeAvro mapToActionTypeAvro(ActionTypeProto actionType) {
        return switch (actionType) {
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case INVERSE -> ActionTypeAvro.INVERSE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Нет такого типа действия");
        };
    }
}