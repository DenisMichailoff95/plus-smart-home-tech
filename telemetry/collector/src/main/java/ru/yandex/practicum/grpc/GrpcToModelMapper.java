package ru.yandex.practicum.grpc;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.model.HubEvent;
import ru.yandex.practicum.model.SensorEvent;
import ru.yandex.practicum.model.hub.DeviceAddedEvent;
import ru.yandex.practicum.model.hub.DeviceRemovedEvent;
import ru.yandex.practicum.model.hub.ScenarioAddedEvent;
import ru.yandex.practicum.model.hub.ScenarioRemovedEvent;
import ru.yandex.practicum.model.sensor.ClimateSensorEvent;
import ru.yandex.practicum.model.sensor.LightSensorEvent;
import ru.yandex.practicum.model.sensor.MotionSensorEvent;
import ru.yandex.practicum.model.sensor.SwitchSensorEvent;
import ru.yandex.practicum.model.sensor.TemperatureSensorEvent;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class GrpcToModelMapper {

    public SensorEvent toSensorEvent(SensorEventProto proto) {
        SensorEventProto.PayloadCase payloadCase = proto.getPayloadCase();

        switch (payloadCase) {
            case MOTION_SENSOR:
                return toMotionSensorEvent(proto);
            case TEMPERATURE_SENSOR:
                return toTemperatureSensorEvent(proto);
            case LIGHT_SENSOR:
                return toLightSensorEvent(proto);
            case CLIMATE_SENSOR:
                return toClimateSensorEvent(proto);
            case SWITCH_SENSOR:
                return toSwitchSensorEvent(proto);
            default:
                throw new IllegalArgumentException("Unknown sensor event type: " + payloadCase);
        }
    }

    public HubEvent toHubEvent(HubEventProto proto) {
        HubEventProto.PayloadCase payloadCase = proto.getPayloadCase();

        switch (payloadCase) {
            case DEVICE_ADDED:
                return toDeviceAddedEvent(proto);
            case DEVICE_REMOVED:
                return toDeviceRemovedEvent(proto);
            case SCENARIO_ADDED:
                return toScenarioAddedEvent(proto);
            case SCENARIO_REMOVED:
                return toScenarioRemovedEvent(proto);
            default:
                throw new IllegalArgumentException("Unknown hub event type: " + payloadCase);
        }
    }

    private MotionSensorEvent toMotionSensorEvent(SensorEventProto proto) {
        MotionSensorEvent event = new MotionSensorEvent();
        setCommonSensorFields(event, proto);

        MotionSensorProto motionSensor = proto.getMotionSensor();
        event.setLinkQuality(motionSensor.getLinkQuality());
        event.setMotion(motionSensor.getMotion());
        event.setVoltage(motionSensor.getVoltage());

        return event;
    }

    private TemperatureSensorEvent toTemperatureSensorEvent(SensorEventProto proto) {
        TemperatureSensorEvent event = new TemperatureSensorEvent();
        setCommonSensorFields(event, proto);

        TemperatureSensorProto tempSensor = proto.getTemperatureSensor();
        event.setTemperatureC(tempSensor.getTemperatureC());
        event.setTemperatureF(tempSensor.getTemperatureF());

        return event;
    }

    private LightSensorEvent toLightSensorEvent(SensorEventProto proto) {
        LightSensorEvent event = new LightSensorEvent();
        setCommonSensorFields(event, proto);

        LightSensorProto lightSensor = proto.getLightSensor();
        event.setLinkQuality(lightSensor.getLinkQuality());
        event.setLuminosity(lightSensor.getLuminosity());

        return event;
    }

    private ClimateSensorEvent toClimateSensorEvent(SensorEventProto proto) {
        ClimateSensorEvent event = new ClimateSensorEvent();
        setCommonSensorFields(event, proto);

        ClimateSensorProto climateSensor = proto.getClimateSensor();
        event.setTemperatureC(climateSensor.getTemperatureC());
        event.setHumidity(climateSensor.getHumidity());
        event.setCo2Level(climateSensor.getCo2Level());

        return event;
    }

    private SwitchSensorEvent toSwitchSensorEvent(SensorEventProto proto) {
        SwitchSensorEvent event = new SwitchSensorEvent();
        setCommonSensorFields(event, proto);

        SwitchSensorProto switchSensor = proto.getSwitchSensor();
        event.setState(switchSensor.getState());

        return event;
    }

    private DeviceAddedEvent toDeviceAddedEvent(HubEventProto proto) {
        DeviceAddedEvent event = new DeviceAddedEvent();
        setCommonHubFields(event, proto);

        DeviceAddedEventProto deviceAdded = proto.getDeviceAdded();
        event.setId(deviceAdded.getId());
        event.setDeviceType(deviceAdded.getType().name());

        return event;
    }

    private DeviceRemovedEvent toDeviceRemovedEvent(HubEventProto proto) {
        DeviceRemovedEvent event = new DeviceRemovedEvent();
        setCommonHubFields(event, proto);

        DeviceRemovedEventProto deviceRemoved = proto.getDeviceRemoved();
        event.setId(deviceRemoved.getId());

        return event;
    }

    private ScenarioAddedEvent toScenarioAddedEvent(HubEventProto proto) {
        ScenarioAddedEvent event = new ScenarioAddedEvent();
        setCommonHubFields(event, proto);

        ScenarioAddedEventProto scenarioAdded = proto.getScenarioAdded();
        event.setName(scenarioAdded.getName());
        event.setConditions(convertConditions(scenarioAdded.getConditionList()));
        event.setActions(convertActions(scenarioAdded.getActionList()));

        return event;
    }

    private ScenarioRemovedEvent toScenarioRemovedEvent(HubEventProto proto) {
        ScenarioRemovedEvent event = new ScenarioRemovedEvent();
        setCommonHubFields(event, proto);

        ScenarioRemovedEventProto scenarioRemoved = proto.getScenarioRemoved();
        event.setName(scenarioRemoved.getName());

        return event;
    }

    private void setCommonSensorFields(SensorEvent event, SensorEventProto proto) {
        event.setId(proto.getId());
        event.setHubId(proto.getHubId());
        event.setTimestamp(Instant.ofEpochSecond(
                proto.getTimestamp().getSeconds(),
                proto.getTimestamp().getNanos()
        ));
    }

    private void setCommonHubFields(HubEvent event, HubEventProto proto) {
        event.setHubId(proto.getHubId());
        event.setTimestamp(Instant.ofEpochSecond(
                proto.getTimestamp().getSeconds(),
                proto.getTimestamp().getNanos()
        ));
    }

    private List<ru.yandex.practicum.model.hub.ScenarioCondition> convertConditions(
            List<ScenarioConditionProto> conditionProtos) {
        return conditionProtos.stream()
                .map(this::convertCondition)
                .collect(Collectors.toList());
    }

    private ru.yandex.practicum.model.hub.ScenarioCondition convertCondition(ScenarioConditionProto proto) {
        ru.yandex.practicum.model.hub.ScenarioCondition condition =
                new ru.yandex.practicum.model.hub.ScenarioCondition();

        condition.setSensorId(proto.getSensorId());
        condition.setType(proto.getType().name());
        condition.setOperation(proto.getOperation().name());

        switch (proto.getValueCase()) {
            case BOOL_VALUE:
                condition.setValue(proto.getBoolValue());
                break;
            case INT_VALUE:
                condition.setValue(proto.getIntValue());
                break;
            default:
                condition.setValue(null);
        }

        return condition;
    }

    private List<ru.yandex.practicum.model.hub.DeviceAction> convertActions(
            List<DeviceActionProto> actionProtos) {
        return actionProtos.stream()
                .map(this::convertAction)
                .collect(Collectors.toList());
    }

    private ru.yandex.practicum.model.hub.DeviceAction convertAction(DeviceActionProto proto) {
        ru.yandex.practicum.model.hub.DeviceAction action =
                new ru.yandex.practicum.model.hub.DeviceAction();

        action.setSensorId(proto.getSensorId());
        action.setType(proto.getType().name());

        if (proto.hasValue()) {
            action.setValue(proto.getValue());
        }

        return action;
    }
}
