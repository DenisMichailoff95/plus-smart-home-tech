package ru.yandex.practicum.collector.schemas.sensorEvent;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import ru.yandex.practicum.collector.enums.SensorEventType;

@Getter
@Schema(description = "Событие датчика движения.")
public class MotionSensorEvent extends BaseSensorEvent {

    @NotNull
    private Integer linkQuality;
    @NotNull
    private Boolean motion;
    @NotNull
    private Integer voltage;

    @Override
    public SensorEventType getType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }
}
