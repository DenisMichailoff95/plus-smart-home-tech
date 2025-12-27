package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

import java.util.UUID;

public class NoOrderFoundException extends BaseException {
    public NoOrderFoundException() {
        super(HttpStatus.BAD_REQUEST,
                "Order not found",
                "Order not found");
    }

    public NoOrderFoundException(UUID orderId) {
        super(HttpStatus.BAD_REQUEST,
                "Order not found with id: " + orderId,
                "Order not found");
    }
}