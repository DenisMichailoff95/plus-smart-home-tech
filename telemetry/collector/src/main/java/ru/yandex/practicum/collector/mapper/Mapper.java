package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;

import java.util.List;

public class Mapper {

    public static List<ScenarioConditionAvro> mapToScenarioConditionAvro(List<ScenarioConditionProto> scenarioConditions) {
        return scenarioConditions.stream()
                .map(scenarioCondition -> new ScenarioConditionAvro(
                        scenarioCondition.getSensorId(),
                        Converter.mapToConditionTypeAvro(scenarioCondition.getType()),
                        Converter.mapToConditionOperationAvro(scenarioCondition.getOperation()),
                        getValueScenarioCondition(scenarioCondition)
                ))
                .toList();
    }

    public static List<DeviceActionAvro> mapToDeviceActionAvro(List<DeviceActionProto> deviceActions) {
        return deviceActions.stream()
                .map(deviceAction -> new DeviceActionAvro(
                        deviceAction.getSensorId(),
                        Converter.mapToActionTypeAvro(deviceAction.getType()),
                        getValueDeviceAction(deviceAction)
                ))
                .toList();
    }

    private static Object getValueScenarioCondition(ScenarioConditionProto scenarioConditionProto) {
        if (scenarioConditionProto.hasBoolValue()) {
            return scenarioConditionProto.getBoolValue();
        } else if (scenarioConditionProto.hasIntValue()) {
            return scenarioConditionProto.getIntValue();
        } else {
            return null;
        }
    }

    private static Integer getValueDeviceAction(DeviceActionProto deviceActionProto) {
        if (deviceActionProto.hasValue()) {
            return deviceActionProto.getValue();
        } else {
            return null;
        }
    }
}
