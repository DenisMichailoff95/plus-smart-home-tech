package ru.yandex.practicum.collector.schemas.sensorEvent;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import ru.yandex.practicum.collector.enums.SensorEventType;

@Getter
@Schema(description = "Событие датчика температуры, содержащее информацию о температуре в градусах Цельсия и Фаренгейта.")
public class TemperatureSensorEvent extends BaseSensorEvent {

    @NotNull
    private int temperatureC;
    @NotNull
    private int temperatureF;

    @Override
    public SensorEventType getType() {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT;
    }
}
