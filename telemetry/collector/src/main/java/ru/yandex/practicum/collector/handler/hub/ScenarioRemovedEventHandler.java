package ru.yandex.practicum.collector.handler.hub;


import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.kafka.CollectorTopics;
import ru.yandex.practicum.collector.kafka.EventProducer;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;


import java.time.Instant;

@Component
@RequiredArgsConstructor
public class ScenarioRemovedEventHandler implements HubEventHandler {
    private final EventProducer producer;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }

    @Override
    public void handle(HubEventProto request) {
        ScenarioRemovedEventProto scenarioRemovedEventProto = request.getScenarioRemoved();
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                CollectorTopics.TELEMETRY_HUBS_V1,
                null,
                Instant.now().toEpochMilli(),
                request.getHubId(),
                new HubEventAvro(
                        request.getHubId(),
                        Instant.ofEpochSecond(request.getTimestamp().getSeconds(), request.getTimestamp().getNanos()),
                        new ScenarioRemovedEventAvro(
                                scenarioRemovedEventProto.getName()
                        )
                )
        );
        producer.getProducer().send(record);
    }
}