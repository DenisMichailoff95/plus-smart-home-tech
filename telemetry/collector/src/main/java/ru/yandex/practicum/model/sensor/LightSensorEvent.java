package ru.yandex.practicum.model.sensor;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.yandex.practicum.model.SensorEvent;

@Getter
@Setter
@ToString(callSuper = true)
public class LightSensorEvent extends SensorEvent {

    private Integer linkQuality;

    private Integer luminosity;

    @Override
    public String getType() {
        return "LIGHT_SENSOR_EVENT";
    }
}