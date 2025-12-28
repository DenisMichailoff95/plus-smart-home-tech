package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class NoSpecifiedProductInWarehouseException extends BaseException {
    public NoSpecifiedProductInWarehouseException() {
        super(HttpStatus.BAD_REQUEST,
                "No specified product in warehouse",
                "No specified product in warehouse");
    }
}