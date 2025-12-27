package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class NotAuthorizedUserException extends BaseException {
    public NotAuthorizedUserException() {
        super(HttpStatus.UNAUTHORIZED,
                "Username must not be empty",
                "Username must not be empty");
    }
}