package ru.yandex.practicum.collector.service;

import org.springframework.stereotype.Service;

import ru.yandex.practicum.collector.handler.sensor.SensorEventHandler;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public final class SensorEventService {

    private final Map<SensorEventProto.PayloadCase, SensorEventHandler> handlers;

    public SensorEventService(List<SensorEventHandler> handlersList) {
        this.handlers = handlersList.stream().collect(
                Collectors.toMap(SensorEventHandler::getMessageType, h -> h)
        );
    }

    public void handle(SensorEventProto proto) {
        SensorEventHandler handler = handlers.get(proto.getPayloadCase());
        if (handler == null) {
            throw new IllegalStateException("Unknown sensor event: " + proto.getPayloadCase());
        }
        handler.handle(proto);
    }
}