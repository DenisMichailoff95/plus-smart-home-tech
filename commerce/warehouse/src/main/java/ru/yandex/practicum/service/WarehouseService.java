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
import ru.yandex.practicum.repository.WarehouseItemRepository;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class WarehouseService {

    private final WarehouseItemRepository warehouseItemRepository;

    @Transactional
    public void addNewProduct(NewProductInWarehouseRequest request) {
        log.info("[WarehouseService] Adding new product to warehouse inventory: {}", request.getProductId());
        log.debug("[WarehouseService] Product details: dimensions={}, weight={}, fragile={}",
                request.getDimension(), request.getWeight(), request.getFragile());

        long startTime = System.currentTimeMillis();
        try {
            if (warehouseItemRepository.findByProductId(request.getProductId()).isPresent()) {
                log.error("[WarehouseService] Product already exists in warehouse: {}", request.getProductId());
                throw new IllegalArgumentException("Product already exists in warehouse: " + request.getProductId());
            }

            WarehouseItem item = new WarehouseItem();
            item.setProductId(request.getProductId());
            item.setQuantity(0);
            log.debug("[WarehouseService] Initialized warehouse item with zero quantity");

            if (request.getDimension() != null) {
                item.setWidth(request.getDimension().getWidth());
                item.setHeight(request.getDimension().getHeight());
                item.setDepth(request.getDimension().getDepth());
                log.debug("[WarehouseService] Dimensions set: width={}, height={}, depth={}",
                        item.getWidth(), item.getHeight(), item.getDepth());
            }

            item.setWeight(request.getWeight());
            item.setFragile(request.getFragile() != null ? request.getFragile() : false);

            log.debug("[WarehouseService] Final item state before save: {}", item);

            warehouseItemRepository.save(item);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[WarehouseService] New product added to warehouse successfully in {} ms", duration);
        } catch (Exception e) {
            log.error("[WarehouseService] Error adding new product: {}", request.getProductId(), e);
            throw e;
        }
    }

    public BookedProductsDto checkProductAvailability(ShoppingCartDto shoppingCart) {
        log.info("[WarehouseService] Checking product availability for shopping cart: {}",
                shoppingCart.getShoppingCartId());
        log.debug("[WarehouseService] Cart contains {} products", shoppingCart.getProducts().size());

        long startTime = System.currentTimeMillis();
        try {
            Double totalVolume = 0.0;
            Double totalWeight = 0.0;
            Boolean hasFragile = false;
            int checkedProducts = 0;

            for (Map.Entry<String, Integer> entry : shoppingCart.getProducts().entrySet()) {
                UUID productId;
                try {
                    productId = UUID.fromString(entry.getKey());
                    log.debug("[WarehouseService] Checking product: {}, quantity: {}",
                            productId, entry.getValue());
                } catch (IllegalArgumentException e) {
                    log.warn("[WarehouseService] Invalid product ID format: {}, skipping", entry.getKey());
                    continue;
                }

                Integer requestedQuantity = entry.getValue();
                checkedProducts++;

                WarehouseItem item = warehouseItemRepository.findByProductId(productId)
                        .orElseThrow(() -> {
                            log.error("[WarehouseService] Product not found in warehouse: {}", productId);
                            return new WarehouseItemNotFoundException(productId);
                        });

                if (item.getQuantity() < requestedQuantity) {
                    log.error("[WarehouseService] Insufficient stock for product: {}. Available: {}, Requested: {}",
                            productId, item.getQuantity(), requestedQuantity);
                    throw new IllegalArgumentException("Insufficient stock for product: " + productId +
                            " (available: " + item.getQuantity() +
                            ", requested: " + requestedQuantity + ")");
                }

                log.debug("[WarehouseService] Product {} available. Stock: {}",
                        productId, item.getQuantity());

                if (item.getWidth() != null && item.getHeight() != null && item.getDepth() != null) {
                    Double volume = item.getWidth().doubleValue() * item.getHeight().doubleValue() * item.getDepth().doubleValue();
                    totalVolume += volume * requestedQuantity;
                    log.debug("[WarehouseService] Product volume: {} m³", volume);
                }

                if (item.getWeight() != null) {
                    totalWeight += item.getWeight().doubleValue() * requestedQuantity;
                    log.debug("[WarehouseService] Product weight: {} kg", item.getWeight());
                }

                if (item.getFragile() != null && item.getFragile()) {
                    hasFragile = true;
                    log.debug("[WarehouseService] Product marked as fragile");
                }
            }

            BookedProductsDto result = new BookedProductsDto(totalVolume, totalWeight, hasFragile);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[WarehouseService] Availability check completed. Products checked: {}, Time: {} ms",
                    checkedProducts, duration);
            log.debug("[WarehouseService] Booking details - Volume: {} m³, Weight: {} kg, Fragile: {}",
                    totalVolume, totalWeight, hasFragile);

            return result;
        } catch (Exception e) {
            log.error("[WarehouseService] Error checking product availability for cart: {}",
                    shoppingCart.getShoppingCartId(), e);
            throw e;
        }
    }

    @Transactional
    public void addProductQuantity(AddProductToWarehouseRequest request) {
        log.info("[WarehouseService] Adding quantity to existing product: {}, quantity: {}",
                request.getProductId(), request.getQuantity());

        long startTime = System.currentTimeMillis();
        try {
            WarehouseItem item = warehouseItemRepository.findByProductId(request.getProductId())
                    .orElseThrow(() -> {
                        log.error("[WarehouseService] Product not found for quantity addition: {}",
                                request.getProductId());
                        return new WarehouseItemNotFoundException(request.getProductId());
                    });

            int oldQuantity = item.getQuantity();
            item.setQuantity(item.getQuantity() + request.getQuantity().intValue());
            warehouseItemRepository.save(item);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[WarehouseService] Quantity updated for product: {}. Old: {}, New: {}, Time: {} ms",
                    request.getProductId(), oldQuantity, item.getQuantity(), duration);
        } catch (Exception e) {
            log.error("[WarehouseService] Error adding quantity to product: {}",
                    request.getProductId(), e);
            throw e;
        }
    }

    public AddressDto getWarehouseAddress() {
        log.info("[WarehouseService] Retrieving warehouse address");

        long startTime = System.currentTimeMillis();
        try {
            String addressValue = Math.random() > 0.5 ? "ADDRESS_1" : "ADDRESS_2";
            log.debug("[WarehouseService] Selected address variant: {}", addressValue);

            AddressDto address = new AddressDto();
            address.setCountry(addressValue);
            address.setCity(addressValue);
            address.setStreet(addressValue);
            address.setHouse(addressValue);
            address.setFlat(addressValue);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[WarehouseService] Address retrieved in {} ms", duration);
            log.debug("[WarehouseService] Address details: {}", address);

            return address;
        } catch (Exception e) {
            log.error("[WarehouseService] Error retrieving warehouse address", e);

            AddressDto defaultAddress = new AddressDto();
            defaultAddress.setCountry("ADDRESS_1");
            defaultAddress.setCity("ADDRESS_1");
            defaultAddress.setStreet("ADDRESS_1");
            defaultAddress.setHouse("ADDRESS_1");
            defaultAddress.setFlat("ADDRESS_1");

            log.warn("[WarehouseService] Returning default address due to error");
            return defaultAddress;
        }
    }
}