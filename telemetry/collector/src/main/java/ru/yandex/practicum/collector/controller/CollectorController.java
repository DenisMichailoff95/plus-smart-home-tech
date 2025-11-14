package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.collector.schemas.hubEvent.BaseHubEvent;
import ru.yandex.practicum.collector.schemas.sensorEvent.BaseSensorEvent;
import ru.yandex.practicum.collector.service.CollectorService;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/events")
public class CollectorController {

    private final CollectorService collectorService;

    @PostMapping("/sensors")
    public ResponseEntity<Void> collectSensorEvent(@Valid @RequestBody BaseSensorEvent sensor) {
        try {
            log.info("Received sensor event - Type: {}, Hub: {}, Sensor: {}",
                    sensor.getType(), sensor.getHubId(), sensor.getId());
            collectorService.collectSensorEvent(sensor);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Error processing sensor event - Type: {}, Hub: {}, Sensor: {}. Error: {}",
                    sensor.getType(), sensor.getHubId(), sensor.getId(), e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/hubs")
    public ResponseEntity<Void> collectHubEvent(@Valid @RequestBody BaseHubEvent hub) {
        try {
            log.info("Received hub event - Type: {}, Hub: {}",
                    hub.getType(), hub.getHubId());
            collectorService.collectHubEvent(hub);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Error processing hub event - Type: {}, Hub: {}. Error: {}",
                    hub.getType(), hub.getHubId(), e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}