package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class InsufficientStockException extends BaseException {
    public InsufficientStockException() {
        super(HttpStatus.BAD_REQUEST,
                "Insufficient stock for requested products",
                "Some products are not available in sufficient quantity");
    }

    public InsufficientStockException(String message) {
        super(HttpStatus.BAD_REQUEST,
                message,
                "Some products are not available in sufficient quantity");
    }
}