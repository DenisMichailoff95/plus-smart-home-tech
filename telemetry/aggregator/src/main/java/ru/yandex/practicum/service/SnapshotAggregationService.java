package ru.yandex.practicum.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class SnapshotAggregationService {

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String deviceId = event.getId();

        log.debug("Updating state for hub: {}, device: {}", hubId, deviceId);

        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(hubId, this::createEmptySnapshot);

        if (!shouldUpdate(snapshot, deviceId, event)) {
            log.debug("No update needed for device: {}", deviceId);
            return Optional.empty();
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        Map<String, SensorStateAvro> newStateMap = new HashMap<>(snapshot.getSensorsState());
        newStateMap.put(deviceId, newState);

        SensorsSnapshotAvro updatedSnapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(event.getTimestamp())
                .setSensorsState(newStateMap)
                .build();

        snapshots.put(hubId, updatedSnapshot);

        log.info("Snapshot updated for hub: {}, device: {}", hubId, deviceId);
        return Optional.of(updatedSnapshot);
    }

    private boolean shouldUpdate(SensorsSnapshotAvro snapshot, String deviceId, SensorEventAvro event) {
        if (!snapshot.getSensorsState().containsKey(deviceId)) {
            log.debug("New device detected: {}", deviceId);
            return true;
        }

        SensorStateAvro oldState = snapshot.getSensorsState().get(deviceId);

        if (oldState.getTimestamp() > event.getTimestamp()) {
            log.debug("Ignoring older event for device: {}. Current: {}, Event: {}",
                    deviceId, oldState.getTimestamp(), event.getTimestamp());
            return false;
        }

        if (oldState.getTimestamp() == event.getTimestamp()) {
            if (dataEquals(oldState.getData(), event.getPayload())) {
                log.debug("Duplicate event with same timestamp and data for device: {}", deviceId);
                return false;
            }
            log.debug("Event with same timestamp but different data for device: {}", deviceId);
        }

        if (dataEquals(oldState.getData(), event.getPayload())) {
            log.debug("Data unchanged but updating timestamp for device: {}", deviceId);
            return true;
        }

        log.debug("Data changed for device: {}", deviceId);
        return true;
    }

    private SensorsSnapshotAvro createEmptySnapshot(String hubId) {
        return SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(0L)
                .setSensorsState(new HashMap<>())
                .build();
    }

    private boolean dataEquals(Object oldData, Object newData) {
        if (oldData == null && newData == null) return true;
        if (oldData == null || newData == null) return false;
        return oldData.equals(newData);
    }

    public SensorsSnapshotAvro getSnapshot(String hubId) {
        return snapshots.get(hubId);
    }

    public int getSnapshotCount() {
        return snapshots.size();
    }

    public void clearSnapshots() {
        snapshots.clear();
        log.info("All snapshots cleared");
    }
}