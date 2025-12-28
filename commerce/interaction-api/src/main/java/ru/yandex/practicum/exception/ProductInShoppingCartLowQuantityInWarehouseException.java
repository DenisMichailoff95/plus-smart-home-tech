package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class ProductInShoppingCartLowQuantityInWarehouseException extends BaseException {
    public ProductInShoppingCartLowQuantityInWarehouseException() {
        super(HttpStatus.BAD_REQUEST,
                "Insufficient quantity of product in warehouse",
                "Insufficient quantity of product in warehouse");
    }
}