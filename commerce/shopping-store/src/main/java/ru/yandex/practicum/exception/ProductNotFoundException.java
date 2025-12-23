package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

import java.util.UUID;

public class ProductNotFoundException extends BaseException {
    public ProductNotFoundException(UUID productId) {
        super(HttpStatus.NOT_FOUND,
                "Product not found with id: " + productId,
                "Product not found");
    }
}