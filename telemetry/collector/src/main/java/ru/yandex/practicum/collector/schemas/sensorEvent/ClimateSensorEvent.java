package ru.yandex.practicum.collector.schemas.sensorEvent;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import ru.yandex.practicum.collector.enums.SensorEventType;

@Getter
@Schema(description = "Событие датчика контроля климата.")
public class ClimateSensorEvent extends BaseSensorEvent {

    @NotNull
    private int temperatureC;
    @NotNull
    private int humidity;
    @NotNull
    private int co2Level;

    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}
