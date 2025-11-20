package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.entity.Scenario;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotAnalysisService {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioExecutionService scenarioExecutionService;

    public void analyzeSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        log.info("=== STARTING SNAPSHOT ANALYSIS FOR HUB: {} ===", hubId);

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        log.info("Found {} scenarios for hub: {}", scenarios.size(), hubId);

        if (scenarios.isEmpty()) {
            log.info("No scenarios found for hub: {}", hubId);
            return;
        }

        scenarioExecutionService.executeScenarios(snapshot, scenarios);
        log.info("=== COMPLETED SNAPSHOT ANALYSIS FOR HUB: {} ===", hubId);
    }
}