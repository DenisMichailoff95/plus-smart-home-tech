package ru.yandex.practicum.model.sensor;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.yandex.practicum.model.SensorEvent;

@Getter
@Setter
@ToString(callSuper = true)
public class TemperatureSensorEvent extends SensorEvent {

    private int temperatureC;

    private int temperatureF;

    @Override
    public String getType() {
        return "TEMPERATURE_SENSOR_EVENT";
    }
}