package ru.yandex.practicum.collector.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.kafka.CollectorTopics;
import ru.yandex.practicum.collector.kafka.EventProducer;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class TemperatureSensorEventHandler implements SensorEventHandler {
    private final EventProducer producer;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR;
    }

    @Override
    public void handle(SensorEventProto request) {
        TemperatureSensorProto temperatureSensorProto = request.getTemperatureSensor();
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                CollectorTopics.TELEMETRY_SENSORS_V1,
                null,
                Instant.now().toEpochMilli(),
                request.getHubId(),
                new SensorEventAvro(
                        request.getId(),
                        request.getHubId(),
                        Instant.ofEpochSecond(request.getTimestamp().getSeconds(), request.getTimestamp().getNanos()),
                        new TemperatureSensorAvro(
                                temperatureSensorProto.getTemperatureC(),
                                temperatureSensorProto.getTemperatureF()
                        )
                )
        );
        producer.getProducer().send(record);
    }
}