package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class NotEnoughInfoInOrderToCalculateException extends BaseException {
    public NotEnoughInfoInOrderToCalculateException() {
        super(HttpStatus.BAD_REQUEST,
                "Not enough information in order to calculate",
                "Not enough information in order to calculate");
    }

    public NotEnoughInfoInOrderToCalculateException(String message) {
        super(HttpStatus.BAD_REQUEST,
                message,
                "Not enough information in order to calculate");
    }
}