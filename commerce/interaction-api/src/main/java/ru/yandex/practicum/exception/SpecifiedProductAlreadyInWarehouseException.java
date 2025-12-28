package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class SpecifiedProductAlreadyInWarehouseException extends BaseException {
    public SpecifiedProductAlreadyInWarehouseException() {
        super(HttpStatus.BAD_REQUEST,
                "Product already exists in warehouse",
                "Product already exists in warehouse");
    }
}