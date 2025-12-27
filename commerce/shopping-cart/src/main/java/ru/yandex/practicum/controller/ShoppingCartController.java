package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.shoppingcart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.shoppingcart.ShoppingCartDto;
import ru.yandex.practicum.service.CartService;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-cart")
public class ShoppingCartController {

    private final CartService cartService;

    @GetMapping
    public ShoppingCartDto getShoppingCart(@RequestParam("username") String username) {
        return cartService.getShoppingCart(username);
    }

    @DeleteMapping
    public void deactivateCart(@RequestParam("username") String username) {
        cartService.deactivateCart(username);
    }

    @PutMapping
    public ShoppingCartDto addProductsToCart(
            @RequestParam("username") String username,
            @RequestBody Map<String, Integer> products) {
        return cartService.addProductsToCart(username, products);
    }

    @PostMapping("/remove")
    public ShoppingCartDto removeProductsFromCart(
            @RequestParam("username") String username,
            @RequestBody List<UUID> productIds) {
        return cartService.removeProductsFromCart(username, productIds);
    }

    @PostMapping("/change-quantity")
    public ShoppingCartDto changeProductQuantity(
            @RequestParam("username") String username,
            @RequestBody ChangeProductQuantityRequest request) {
        return cartService.changeProductQuantity(username, request);
    }
}