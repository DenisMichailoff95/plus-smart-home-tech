package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class NoProductsInShoppingCartException extends BaseException {
    public NoProductsInShoppingCartException() {
        super(HttpStatus.BAD_REQUEST,
                "No products found in shopping cart",
                "No products found in shopping cart");
    }
}