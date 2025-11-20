package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.stereotype.Service;

import ru.yandex.practicum.kafka.telemetry.event.ActionType;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperation;
import ru.yandex.practicum.kafka.telemetry.event.ConditionType;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAction;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceType;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioCondition;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventMapperService {

    public byte[] toAvroBytes(SensorEvent event) {
        try {
            SensorEventAvro avroEvent = convertSensorEventToAvro(event);
            byte[] result = serializeAvro(avroEvent, SensorEventAvro.getClassSchema());
            log.debug("Converted sensor event to {} bytes", result.length);
            return result;
        } catch (Exception e) {
            log.error("Failed to convert sensor event to Avro: {}", event, e);
            throw new RuntimeException("Failed to convert sensor event to Avro", e);
        }
    }

    public byte[] toAvroBytes(HubEvent event) {
        try {
            HubEventAvro avroEvent = convertHubEventToAvro(event);
            byte[] result = serializeAvro(avroEvent, HubEventAvro.getClassSchema());
            log.debug("Converted hub event to {} bytes", result.length);
            return result;
        } catch (Exception e) {
            log.error("Failed to convert hub event to Avro: {}", event, e);
            throw new RuntimeException("Failed to convert hub event to Avro", e);
        }
    }

    private <T> byte[] serializeAvro(T avroObject, org.apache.avro.Schema schema) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<T> writer = new SpecificDatumWriter<>(schema);

        writer.write(avroObject, encoder);
        encoder.flush();
        out.close();

        return out.toByteArray();
    }

    private SensorEventAvro convertSensorEventToAvro(SensorEvent event) {
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setPayload(createSensorPayload(event))
                .build();
    }

    private Object createSensorPayload(SensorEvent event) {
        if (event instanceof ClimateSensorEvent) {
            ClimateSensorEvent climateEvent = (ClimateSensorEvent) event;
            return ClimateSensorAvro.newBuilder()
                    .setTemperatureC(climateEvent.getTemperatureC())
                    .setHumidity(climateEvent.getHumidity())
                    .setCo2Level(climateEvent.getCo2Level())
                    .build();
        } else if (event instanceof LightSensorEvent) {
            LightSensorEvent lightEvent = (LightSensorEvent) event;
            LightSensorAvro.Builder builder = LightSensorAvro.newBuilder();

            if (lightEvent.getLinkQuality() != null) {
                builder.setLinkQuality(lightEvent.getLinkQuality());
            } else {
                builder.setLinkQuality(0);
            }

            if (lightEvent.getLuminosity() != null) {
                builder.setLuminosity(lightEvent.getLuminosity());
            } else {
                builder.setLuminosity(0);
            }

            return builder.build();
        } else if (event instanceof MotionSensorEvent) {
            MotionSensorEvent motionEvent = (MotionSensorEvent) event;
            return MotionSensorAvro.newBuilder()
                    .setLinkQuality(motionEvent.getLinkQuality())
                    .setMotion(motionEvent.isMotion())
                    .setVoltage(motionEvent.getVoltage())
                    .build();
        } else if (event instanceof SwitchSensorEvent) {
            SwitchSensorEvent switchEvent = (SwitchSensorEvent) event;
            return SwitchSensorAvro.newBuilder()
                    .setState(switchEvent.isState())
                    .build();
        } else if (event instanceof TemperatureSensorEvent) {
            TemperatureSensorEvent tempEvent = (TemperatureSensorEvent) event;
            return TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(tempEvent.getTemperatureC())
                    .setTemperatureF(tempEvent.getTemperatureF())
                    .build();
        }

        throw new IllegalArgumentException("Unknown sensor event type: " + event.getClass().getSimpleName());
    }

    private HubEventAvro convertHubEventToAvro(HubEvent event) {
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli())
                .setPayload(createHubPayload(event))
                .build();
    }

    private Object createHubPayload(HubEvent event) {
        if (event instanceof DeviceAddedEvent) {
            DeviceAddedEvent deviceAdded = (DeviceAddedEvent) event;
            return DeviceAddedEventAvro.newBuilder()
                    .setId(deviceAdded.getId())
                    .setDeviceType(mapDeviceType(deviceAdded.getDeviceType()))
                    .build();
        } else if (event instanceof DeviceRemovedEvent) {
            DeviceRemovedEvent deviceRemoved = (DeviceRemovedEvent) event;
            return DeviceRemovedEventAvro.newBuilder()
                    .setId(deviceRemoved.getId())
                    .build();
        } else if (event instanceof ScenarioAddedEvent) {
            ScenarioAddedEvent scenarioAdded = (ScenarioAddedEvent) event;
            return ScenarioAddedEventAvro.newBuilder()
                    .setName(scenarioAdded.getName())
                    .setConditions(convertConditions(scenarioAdded.getConditions()))
                    .setActions(convertActions(scenarioAdded.getActions()))
                    .build();
        } else if (event instanceof ScenarioRemovedEvent) {
            ScenarioRemovedEvent scenarioRemoved = (ScenarioRemovedEvent) event;
            return ScenarioRemovedEventAvro.newBuilder()
                    .setName(scenarioRemoved.getName())
                    .build();
        }

        throw new IllegalArgumentException("Unknown hub event type: " + event.getClass().getSimpleName());
    }

    private DeviceType mapDeviceType(String deviceType) {
        switch (deviceType.toUpperCase()) {
            case "MOTION_SENSOR":
                return DeviceType.MOTION_SENSOR;
            case "LIGHT_SENSOR":
                return DeviceType.LIGHT_SENSOR;
            case "SWITCH_SENSOR":
                return DeviceType.SWITCH_SENSOR;
            case "CLIMATE_SENSOR":
                return DeviceType.CLIMATE_SENSOR;
            case "TEMPERATURE_SENSOR":
                return DeviceType.TEMPERATURE_SENSOR;
            default:
                log.warn("Unknown device type: {}, defaulting to SWITCH_SENSOR", deviceType);
                return DeviceType.SWITCH_SENSOR;
        }
    }

    private ConditionType mapConditionType(String conditionType) {
        switch (conditionType.toUpperCase()) {
            case "MOTION":
                return ConditionType.MOTION;
            case "LUMINOSITY":
                return ConditionType.LUMINOSITY;
            case "SWITCH":
                return ConditionType.SWITCH;
            case "TEMPERATURE":
                return ConditionType.TEMPERATURE;
            case "CO2LEVEL":
                return ConditionType.CO2LEVEL;
            case "HUMIDITY":
                return ConditionType.HUMIDITY;
            default:
                log.warn("Unknown condition type: {}, defaulting to SWITCH", conditionType);
                return ConditionType.SWITCH;
        }
    }

    private ConditionOperation mapConditionOperation(String operation) {
        switch (operation.toUpperCase()) {
            case "EQUALS":
                return ConditionOperation.EQUALS;
            case "GREATER_THAN":
                return ConditionOperation.GREATER_THAN;
            case "LOWER_THAN":
                return ConditionOperation.LOWER_THAN;
            default:
                log.warn("Unknown condition operation: {}, defaulting to EQUALS", operation);
                return ConditionOperation.EQUALS;
        }
    }

    private ActionType mapActionType(String actionType) {
        switch (actionType.toUpperCase()) {
            case "ACTIVATE":
                return ActionType.ACTIVATE;
            case "DEACTIVATE":
                return ActionType.DEACTIVATE;
            case "INVERSE":
                return ActionType.INVERSE;
            case "SET_VALUE":
                return ActionType.SET_VALUE;
            default:
                log.warn("Unknown action type: {}, defaulting to ACTIVATE", actionType);
                return ActionType.ACTIVATE;
        }
    }

    private List<ScenarioCondition> convertConditions(List<ru.yandex.practicum.model.hub.ScenarioCondition> conditions) {
        return conditions.stream()
                .map(this::convertCondition)
                .collect(Collectors.toList());
    }

    private ScenarioCondition convertCondition(ru.yandex.practicum.model.hub.ScenarioCondition condition) {
        ScenarioCondition.Builder builder = ScenarioCondition.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(mapConditionType(condition.getType()))
                .setOperation(mapConditionOperation(condition.getOperation()));

        Object value = condition.getValue();
        if (value != null) {
            if (value instanceof Integer) {
                int intValue = (Integer) value;
                if ("MOTION".equalsIgnoreCase(condition.getType()) || "SWITCH".equalsIgnoreCase(condition.getType())) {
                    builder.setValue(intValue == 1);
                } else {
                    builder.setValue(intValue);
                }
            } else if (value instanceof Boolean) {
                builder.setValue((Boolean) value);
            } else {
                log.warn("Unknown value type: {} for condition type: {}", value.getClass(), condition.getType());
            }
        }

        return builder.build();
    }

    private List<DeviceAction> convertActions(List<ru.yandex.practicum.model.hub.DeviceAction> actions) {
        return actions.stream()
                .map(this::convertAction)
                .collect(Collectors.toList());
    }

    private DeviceAction convertAction(ru.yandex.practicum.model.hub.DeviceAction action) {
        DeviceAction.Builder builder = DeviceAction.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(mapActionType(action.getType()));

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }

    public byte[] snapshotToAvroBytes(SensorsSnapshotAvro snapshot) {
        try {
            byte[] result = serializeAvro(snapshot, SensorsSnapshotAvro.getClassSchema());
            log.debug("Converted snapshot to {} bytes", result.length);
            return result;
        } catch (Exception e) {
            log.error("Failed to convert snapshot to Avro: {}", snapshot, e);
            throw new RuntimeException("Failed to convert snapshot to Avro", e);
        }
    }
}