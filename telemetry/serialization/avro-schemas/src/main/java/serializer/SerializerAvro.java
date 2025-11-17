package serializer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

public class SerializerAvro implements Serializer<SpecificRecordBase> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Конфигурация не требуется
    }

    @Override
    public byte[] serialize(String topic, SpecificRecordBase input) {
        if (input == null) {
            return null;
        }
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<SpecificRecordBase> datumWriter = new SpecificDatumWriter<>(input.getSchema());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

            datumWriter.write(input, encoder);
            encoder.flush();

            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Ошибка сериализации Avro данных для топика: " + topic +
                    ", схема: " + input.getSchema().getFullName() +
                    ", ошибка: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        // Ресурсы не требуют закрытия
    }
}