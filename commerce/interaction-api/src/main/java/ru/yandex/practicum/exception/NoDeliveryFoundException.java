package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

import java.util.UUID;

public class NoDeliveryFoundException extends BaseException {
    public NoDeliveryFoundException(UUID deliveryId) {
        super(HttpStatus.NOT_FOUND,
                "Delivery not found with id: " + deliveryId,
                "Delivery not found");
    }

    public NoDeliveryFoundException() {
        super(HttpStatus.NOT_FOUND,
                "Delivery not found",
                "Delivery not found");
    }
}