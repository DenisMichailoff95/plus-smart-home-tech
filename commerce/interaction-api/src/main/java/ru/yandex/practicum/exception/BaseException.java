package ru.yandex.practicum.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class BaseException extends RuntimeException {
    private final HttpStatus httpStatus;
    private final String userMessage;

    public BaseException(HttpStatus httpStatus, String message, String userMessage) {
        super(message);
        this.httpStatus = httpStatus;
        this.userMessage = userMessage;
    }
}