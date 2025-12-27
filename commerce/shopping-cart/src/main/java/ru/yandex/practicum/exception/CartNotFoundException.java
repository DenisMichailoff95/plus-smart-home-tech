package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

import java.util.UUID;

public class CartNotFoundException extends BaseException {
    public CartNotFoundException(String username, UUID cartId) {
        super(HttpStatus.NOT_FOUND,
                "Cart not found for user: " + username + " with id: " + cartId,
                "Cart not found");
    }

    public CartNotFoundException(String username) {
        super(HttpStatus.NOT_FOUND,
                "Cart not found for user: " + username,
                "Cart not found");
    }
}