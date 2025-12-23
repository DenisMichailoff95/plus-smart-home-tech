package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.shoppingcart.ShoppingCartDto;

import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.service.WarehouseService;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/warehouse")
public class WarehouseController {

    private final WarehouseService warehouseService;

    @PutMapping
    public ResponseEntity<Void> newProductInWarehouse(@RequestBody NewProductInWarehouseRequest request) {
        log.info("Adding new product to warehouse: {}", request.getProductId());
        warehouseService.addNewProduct(request);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/check")
    public ResponseEntity<BookedProductsDto> checkProductQuantityEnoughForShoppingCart(@RequestBody ShoppingCartDto shoppingCart) {
        log.info("Checking product availability for shopping cart: {}", shoppingCart.getShoppingCartId());
        BookedProductsDto result = warehouseService.checkProductAvailability(shoppingCart);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/add")
    public ResponseEntity<Void> addProductToWarehouse(@RequestBody AddProductToWarehouseRequest request) {
        log.info("Adding quantity to existing product: {}, quantity: {}", request.getProductId(), request.getQuantity());
        warehouseService.addProductQuantity(request);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/address")
    public ResponseEntity<AddressDto> getWarehouseAddress() {
        log.info("Getting warehouse address");
        AddressDto address = warehouseService.getWarehouseAddress();
        return ResponseEntity.ok(address);
    }
}