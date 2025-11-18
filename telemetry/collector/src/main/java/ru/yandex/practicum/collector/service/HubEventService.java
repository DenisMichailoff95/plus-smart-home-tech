package ru.yandex.practicum.collector.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.handler.hub.HubEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public final class HubEventService {

    private final Map <HubEventProto.PayloadCase, HubEventHandler> handlers;

    public HubEventService(List<HubEventHandler> handlersList) {
        this.handlers = handlersList.stream().collect(
                Collectors.toMap(HubEventHandler::getMessageType, h -> h)
        );
    }

    public void handle(HubEventProto proto) {
        HubEventHandler handler = handlers.get(proto.getPayloadCase());
        if (handler == null) {
            throw new IllegalStateException("Unknown hub event: " + proto.getPayloadCase());
        }
        handler.handle(proto);
    }
}

