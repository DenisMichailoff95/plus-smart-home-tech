package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class ProductInShoppingCartNotInWarehouseException extends BaseException {
    public ProductInShoppingCartNotInWarehouseException() {
        super(HttpStatus.BAD_REQUEST,
                "Product in shopping cart not found in warehouse",
                "Product in shopping cart not found in warehouse");
    }
}