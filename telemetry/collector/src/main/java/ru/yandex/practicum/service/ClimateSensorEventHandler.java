package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.GrpcToModelMapper;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@Slf4j
@Component
@RequiredArgsConstructor
public class ClimateSensorEventHandler implements SensorEventHandler {

    private final KafkaProducerService kafkaProducerService;
    private final GrpcToModelMapper grpcToModelMapper;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        log.info("Processing climate sensor event from device: {}", event.getId());

        var sensorEvent = grpcToModelMapper.toSensorEvent(event);
        kafkaProducerService.sendSensorEvent(sensorEvent);
    }
}