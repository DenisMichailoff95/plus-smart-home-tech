package ru.yandex.practicum.collector.handler.hub;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.mapper.Converter;
import ru.yandex.practicum.collector.kafka.CollectorTopics;
import ru.yandex.practicum.collector.kafka.EventProducer;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Instant;


@Component
@RequiredArgsConstructor
public class DeviceAddedEventHandler implements HubEventHandler {
    private final EventProducer producer;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    public void handle(HubEventProto request) {
        DeviceAddedEventProto deviceAddedEventProto = request.getDeviceAdded();

        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                CollectorTopics.TELEMETRY_HUBS_V1,
                null,
                Instant.now().toEpochMilli(),
                request.getHubId(),
                new HubEventAvro(
                        request.getHubId(),
                        Instant.ofEpochSecond(request.getTimestamp().getSeconds(), request.getTimestamp().getNanos()),
                        new DeviceAddedEventAvro(
                                deviceAddedEventProto.getId(),
                                Converter.mapToDeviceTypeAvro(deviceAddedEventProto.getType())
                        )
                )
        );

        producer.getProducer().send(record);
    }
}

