package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotAnalysisService {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioExecutionService scenarioExecutionService;
    private final DecoderFactory decoderFactory = DecoderFactory.get();

    public void analyzeSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        log.debug("Starting snapshot analysis for hub: {}", hubId);

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        log.debug("Found {} scenarios for hub: {}", scenarios.size(), hubId);

        if (!scenarios.isEmpty()) {
            scenarioExecutionService.executeScenarios(snapshot, scenarios);
        } else {
            log.debug("No scenarios found for hub: {}", hubId);
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
}