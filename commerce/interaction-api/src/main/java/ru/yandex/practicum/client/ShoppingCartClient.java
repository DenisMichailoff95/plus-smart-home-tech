package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import ru.yandex.practicum.dto.shoppingcart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.shoppingcart.ShoppingCartDto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@FeignClient(name = "shopping-cart")
public interface ShoppingCartClient {

    @GetMapping("/api/v1/shopping-cart")
    ShoppingCartDto getShoppingCart(@RequestParam("username") String username);

    @PutMapping("/api/v1/shopping-cart")
    ShoppingCartDto addProductsToCart(
            @RequestParam("username") String username,
            @RequestBody Map<String, Integer> products
    );

    @DeleteMapping("/api/v1/shopping-cart")
    void deactivateCart(@RequestParam("username") String username);

    @PostMapping("/api/v1/shopping-cart/remove")
    ShoppingCartDto removeProductsFromCart(
            @RequestParam("username") String username,
            @RequestBody List<UUID> productIds
    );

    @PostMapping("/api/v1/shopping-cart/change-quantity")
    ShoppingCartDto changeProductQuantity(
            @RequestParam("username") String username,
            @RequestBody ChangeProductQuantityRequest request
    );
}