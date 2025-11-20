package ru.yandex.practicum.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotAnalysisService;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final SnapshotAnalysisService snapshotAnalysisService;
    private volatile boolean running = true;
    private KafkaConsumer<String, byte[]> consumer;
    private final DecoderFactory decoderFactory = DecoderFactory.get();

    private static final String SNAPSHOTS_TOPIC = "telemetry.snapshots.v1";
    private static final String CONSUMER_GROUP = "analyzer-snapshots";

    public void start() {
        log.info("Starting Snapshot Processor...");
        initializeConsumer();

        try {
            consumer.subscribe(List.of(SNAPSHOTS_TOPIC));
            log.info("Subscribed to topic: {}", SNAPSHOTS_TOPIC);

            while (running) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.debug("Received {} snapshots", records.count());
                    processSnapshots(records);
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            log.error("Error in snapshot processor", e);
        } finally {
            shutdown();
        }
    }

    private void initializeConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", CONSUMER_GROUP);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");

        consumer = new KafkaConsumer<>(props);
    }

    private void processSnapshots(ConsumerRecords<String, byte[]> records) {
        for (ConsumerRecord<String, byte[]> record : records) {
            try {
                SensorsSnapshotAvro snapshot = deserializeSnapshot(record.value());
                snapshotAnalysisService.analyzeSnapshot(snapshot);
                log.debug("Processed snapshot for hub: {}", record.key());
            } catch (Exception e) {
                log.error("Error processing snapshot", e);
            }
        }
    }

    private SensorsSnapshotAvro deserializeSnapshot(byte[] data) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
            BinaryDecoder decoder = decoderFactory.binaryDecoder(inputStream, null);
            SpecificDatumReader<SensorsSnapshotAvro> reader =
                    new SpecificDatumReader<>(SensorsSnapshotAvro.getClassSchema());
            return reader.read(null, decoder);
        }
    }

    public void stop() {
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    private void shutdown() {
        if (consumer != null) {
            consumer.close();
        }
        log.info("Snapshot Processor stopped");
    }
}
