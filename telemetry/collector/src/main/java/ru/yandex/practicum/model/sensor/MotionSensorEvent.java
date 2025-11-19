package ru.yandex.practicum.model.sensor;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.yandex.practicum.model.SensorEvent;

@Getter
@Setter
@ToString(callSuper = true)
public class MotionSensorEvent extends SensorEvent {

    private int linkQuality;

    private boolean motion;

    private int voltage;

    @Override
    public String getType() {
        return "MOTION_SENSOR_EVENT";
    }
}