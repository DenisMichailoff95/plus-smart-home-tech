package ru.yandex.practicum.model.hub;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ScenarioCondition {

    @NotBlank
    private String sensorId;

    @NotBlank
    private String type;

    @NotBlank
    private String operation;

    private Object value;
}
