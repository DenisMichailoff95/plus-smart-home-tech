package ru.yandex.practicum.collector.schemas.hubEvent;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.ToString;
import ru.yandex.practicum.collector.enums.HubEventType;

@Getter
@ToString(callSuper = true)
public class DeviceRemovedEvent extends BaseHubEvent {

    @NotBlank
    private String id;

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_REMOVED;
    }
}
