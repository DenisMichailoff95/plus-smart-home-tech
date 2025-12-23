package ru.yandex.practicum.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;

@Slf4j
public class BaseAvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    private final DecoderFactory decoderFactory;
    private final Schema schema;

    public BaseAvroDeserializer(Schema schema) {
        this(DecoderFactory.get(), schema);
    }

    public BaseAvroDeserializer(DecoderFactory decoderFactory, Schema schema) {
        this.decoderFactory = decoderFactory;
        this.schema = schema;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
            BinaryDecoder decoder = decoderFactory.binaryDecoder(inputStream, null);
            SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);

            T result = reader.read(null, decoder);
            log.debug("Successfully deserialized Avro data for topic: {}", topic);
            return result;

        } catch (IOException e) {
            log.error("Error deserializing Avro data for topic: {}", topic, e);
            throw new RuntimeException("Failed to deserialize Avro data", e);
        }
    }

    @Override
    public void close() {
        // Пустая реализация, но метод требуется интерфейсом
    }
}