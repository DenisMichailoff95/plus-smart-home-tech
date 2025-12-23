package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

import java.util.UUID;

public class WarehouseItemNotFoundException extends BaseException {
    public WarehouseItemNotFoundException(UUID productId) {
        super(HttpStatus.NOT_FOUND,
                "Warehouse item not found for product id: " + productId,
                "Warehouse item not found");
    }
}