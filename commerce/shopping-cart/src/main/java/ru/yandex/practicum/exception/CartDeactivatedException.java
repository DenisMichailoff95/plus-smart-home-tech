package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

import java.util.UUID;

public class CartDeactivatedException extends BaseException {
    public CartDeactivatedException(String username, UUID cartId) {
        super(HttpStatus.BAD_REQUEST,
                "Cart is deactivated for user: " + username + " with id: " + cartId + ". Cannot modify deactivated cart.",
                "Cart is deactivated. Cannot modify deactivated cart.");
    }

    public CartDeactivatedException(String username) {
        super(HttpStatus.BAD_REQUEST,
                "Cart is deactivated for user: " + username + ". Cannot modify deactivated cart.",
                "Cart is deactivated. Cannot modify deactivated cart.");
    }
}