package ru.yandex.practicum.model.sensor;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.yandex.practicum.model.SensorEvent;

@Getter
@Setter
@ToString(callSuper = true)
public class SwitchSensorEvent extends SensorEvent {

    private boolean state;

    @Override
    public String getType() {
        return "SWITCH_SENSOR_EVENT";
    }
}