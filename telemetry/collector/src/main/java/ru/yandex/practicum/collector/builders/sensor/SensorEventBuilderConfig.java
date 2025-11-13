package ru.yandex.practicum.collector.builders.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.collector.enums.SensorEventType;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class SensorEventBuilderConfig {
    private final ClimateSensorBuilder climateSensorBuilder;
    private final LightSensorBuilder lightSensorBuilder;
    private final MotionSensorBuilder motionSensorBuilder;
    private final SwitchSensorBuilder switchSensorBuilder;
    private final TemperatureSensorBuilder temperatureSensorBuilder;

    @Bean
    public Map<SensorEventType, SensorEventBuilder> getSensorEventBuilders() {
        Map<SensorEventType, SensorEventBuilder> sensorEventBuilders = new HashMap<>();

        sensorEventBuilders.put(SensorEventType.SWITCH_SENSOR_EVENT, switchSensorBuilder);
        sensorEventBuilders.put(SensorEventType.CLIMATE_SENSOR_EVENT, climateSensorBuilder);
        sensorEventBuilders.put(SensorEventType.LIGHT_SENSOR_EVENT, lightSensorBuilder);
        sensorEventBuilders.put(SensorEventType.MOTION_SENSOR_EVENT, motionSensorBuilder);
        sensorEventBuilders.put(SensorEventType.TEMPERATURE_SENSOR_EVENT, temperatureSensorBuilder);

        return sensorEventBuilders;
    }
}
