package ru.yandex.practicum.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Slf4j
@Service
public class SnapshotMapperService {

    public byte[] snapshotToAvroBytes(SensorsSnapshotAvro snapshot) {
        try {
            byte[] result = serializeAvro(snapshot, SensorsSnapshotAvro.getClassSchema());
            log.debug("Converted snapshot to {} bytes", result.length);
            return result;
        } catch (Exception e) {
            log.error("Failed to convert snapshot to Avro: {}", snapshot, e);
            throw new RuntimeException("Failed to convert snapshot to Avro", e);
        }
    }

    private <T> byte[] serializeAvro(T avroObject, org.apache.avro.Schema schema) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<T> writer = new SpecificDatumWriter<>(schema);

        writer.write(avroObject, encoder);
        encoder.flush();
        out.close();

        return out.toByteArray();
    }
}