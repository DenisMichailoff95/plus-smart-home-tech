package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

import java.util.UUID;

public class InsufficientQuantityException extends BaseException {
    public InsufficientQuantityException(UUID productId, Integer requested, Integer available) {
        super(HttpStatus.BAD_REQUEST,
                "Insufficient quantity for product: " + productId + ". Requested: " + requested + ", Available: " + available,
                "Insufficient quantity available");
    }
}