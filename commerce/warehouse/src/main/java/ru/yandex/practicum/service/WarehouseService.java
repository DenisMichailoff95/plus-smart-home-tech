package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.shoppingcart.ShoppingCartDto;

import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.entity.WarehouseItem;
import ru.yandex.practicum.exception.WarehouseItemNotFoundException;
import ru.yandex.practicum.mapper.WarehouseMapper;
import ru.yandex.practicum.repository.WarehouseAddressRepository;
import ru.yandex.practicum.repository.WarehouseItemRepository;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class WarehouseService {

    private final WarehouseItemRepository warehouseItemRepository;
    private final WarehouseAddressRepository warehouseAddressRepository;
    private final WarehouseMapper warehouseMapper;

    @Transactional
    public void addNewProduct(NewProductInWarehouseRequest request) {
        log.info("Adding new product to warehouse: {}", request.getProductId());

        if (warehouseItemRepository.findByProductId(request.getProductId()).isPresent()) {
            throw new IllegalArgumentException("Product already exists in warehouse: " + request.getProductId());
        }

        WarehouseItem item = new WarehouseItem();
        item.setProductId(request.getProductId());
        item.setQuantity(0);

        if (request.getDimension() != null) {
            item.setWidth(request.getDimension().getWidth());
            item.setHeight(request.getDimension().getHeight());
            item.setDepth(request.getDimension().getDepth());
        }

        item.setWeight(request.getWeight());
        item.setFragile(request.getFragile() != null ? request.getFragile() : false);

        warehouseItemRepository.save(item);
        log.info("New product added to warehouse: {}", request.getProductId());
    }

    public BookedProductsDto checkProductAvailability(ShoppingCartDto shoppingCart) {
        log.info("Checking availability for shopping cart: {}", shoppingCart.getShoppingCartId());

        Double totalVolume = 0.0;
        Double totalWeight = 0.0;
        Boolean hasFragile = false;

        for (Map.Entry<String, Integer> entry : shoppingCart.getProducts().entrySet()) {
            UUID productId = UUID.fromString(entry.getKey()); // Конвертируем
            Integer requestedQuantity = entry.getValue();

            WarehouseItem item = warehouseItemRepository.findByProductId(productId)
                    .orElseThrow(() -> new WarehouseItemNotFoundException(productId));

            if (item.getQuantity() < requestedQuantity) {
                throw new IllegalArgumentException("Insufficient quantity for product: " + productId);
            }

            if (item.getWidth() != null && item.getHeight() != null && item.getDepth() != null) {
                Double volume = item.getWidth().doubleValue() * item.getHeight().doubleValue() * item.getDepth().doubleValue();
                totalVolume += volume * requestedQuantity;
            }

            if (item.getWeight() != null) {
                totalWeight += item.getWeight().doubleValue() * requestedQuantity;
            }

            if (item.getFragile() != null && item.getFragile()) {
                hasFragile = true;
            }
        }

        return new BookedProductsDto(totalVolume, totalWeight, hasFragile);
    }

    @Transactional
    public void addProductQuantity(AddProductToWarehouseRequest request) {
        log.info("Adding quantity to product: {}, quantity: {}", request.getProductId(), request.getQuantity());

        WarehouseItem item = warehouseItemRepository.findByProductId(request.getProductId())
                .orElseThrow(() -> new WarehouseItemNotFoundException(request.getProductId()));

        item.setQuantity(item.getQuantity() + request.getQuantity().intValue());
        warehouseItemRepository.save(item);

        log.info("Quantity updated for product: {}, new quantity: {}", request.getProductId(), item.getQuantity());
    }

    public AddressDto getWarehouseAddress() {
        log.info("Getting warehouse address");

        try {
            String addressValue = Math.random() > 0.5 ? "ADDRESS_1" : "ADDRESS_2";

            AddressDto address = new AddressDto();
            address.setCountry(addressValue);
            address.setCity(addressValue);
            address.setStreet(addressValue);
            address.setHouse(addressValue);
            address.setFlat(addressValue);

            return address;

        } catch (Exception e) {
            log.error("Error getting warehouse address: {}", e.getMessage(), e);
            AddressDto defaultAddress = new AddressDto();
            defaultAddress.setCountry("ADDRESS_1");
            defaultAddress.setCity("ADDRESS_1");
            defaultAddress.setStreet("ADDRESS_1");
            defaultAddress.setHouse("ADDRESS_1");
            defaultAddress.setFlat("ADDRESS_1");
            return defaultAddress;
        }
    }
}