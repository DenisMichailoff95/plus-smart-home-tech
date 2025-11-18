package ru.yandex.practicum.collector.handler.sensor;

import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface SensorEventHandler {

    // какой PayloadCase обрабатывается
    SensorEventProto.PayloadCase getMessageType ();

    // бизнес-логика
    void handle (SensorEventProto event);
}
