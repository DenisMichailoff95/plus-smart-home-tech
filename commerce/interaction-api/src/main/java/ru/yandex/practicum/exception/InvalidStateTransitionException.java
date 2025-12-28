package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class InvalidStateTransitionException extends BaseException {
    public InvalidStateTransitionException(String message) {
        super(HttpStatus.CONFLICT,
                message,
                "Invalid state transition for order");
    }

    public InvalidStateTransitionException(String currentState, String targetState) {
        super(HttpStatus.CONFLICT,
                String.format("Cannot transition from %s to %s", currentState, targetState),
                "Invalid order state transition");
    }
}