package ru.yandex.practicum.analyzer.handlers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public class HubHandler {

    private final Map<String, HubEventHandler> handlers;

    public HubHandler(Set<HubEventHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getMessageType, Function.identity()));
        log.info("Initialized HubHandler with {} handlers: {}", handlers.size(), this.handlers.keySet());
    }

    public void handleEvent(HubEventAvro event) {
        String payloadName = event.getPayload().getClass().getSimpleName();

        if (handlers.containsKey(payloadName)) {
            try {
                handlers.get(payloadName).handle(event);
                log.debug("Successfully handled hub event type: {}", payloadName);
            } catch (Exception e) {
                log.error("Error handling hub event type: {}", payloadName, e);
                throw e;
            }
        } else {
            log.warn("No handler found for hub event type: {}", payloadName);
            throw new IllegalArgumentException("No handler for hub event type: " + payloadName);
        }
    }
}