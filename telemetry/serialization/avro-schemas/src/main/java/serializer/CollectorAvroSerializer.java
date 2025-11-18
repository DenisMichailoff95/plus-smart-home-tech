package ru.yandex.practicum.serializer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CollectorAvroSerializer implements Serializer<SpecificRecordBase> {
    private final EncoderFactory encoderFactory = EncoderFactory.get();

    @Override
    public byte[] serialize(String topic, SpecificRecordBase data) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] result = null;
            BinaryEncoder encoder = encoderFactory.binaryEncoder(outputStream, null);
            if (data != null) {
                DatumWriter<SpecificRecordBase> datumWriter = new SpecificDatumWriter<>(data.getSchema());
                datumWriter.write(data, encoder);
                encoder.flush();
                result = outputStream.toByteArray();
            }
            return result;
        } catch (IOException e) {
            throw new SerializationException("Ошибка сериализации данных для топика [" + topic + "]", e);
        }
    }
}