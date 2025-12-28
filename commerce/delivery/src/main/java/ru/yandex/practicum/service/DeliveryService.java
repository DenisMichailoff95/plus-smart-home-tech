package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.OrderServiceClient;
import ru.yandex.practicum.client.WarehouseDeliveryClient;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.delivery.DeliveryState;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.warehouse.ShippedToDeliveryRequest;
import ru.yandex.practicum.entity.Delivery;
import ru.yandex.practicum.exception.NoDeliveryFoundException;
import ru.yandex.practicum.mapper.DeliveryMapper;
import ru.yandex.practicum.repository.DeliveryRepository;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryMapper deliveryMapper;
    private final OrderServiceClient orderServiceClient;
    private final WarehouseDeliveryClient warehouseDeliveryClient;

    @Transactional
    public DeliveryDto planDelivery(DeliveryDto deliveryDto) {
        log.info("Planning delivery for order: {}", deliveryDto.getOrderId());

        if (deliveryRepository.existsByOrderId(deliveryDto.getOrderId())) {
            log.error("Delivery already exists for order: {}", deliveryDto.getOrderId());
            throw new IllegalArgumentException("Delivery already exists for order: " + deliveryDto.getOrderId());
        }

        // Validate delivery addresses
        validateDeliveryAddress(deliveryDto);

        Delivery delivery = deliveryMapper.toEntity(deliveryDto);
        delivery.setDeliveryState(DeliveryState.CREATED);

        Delivery savedDelivery = deliveryRepository.save(delivery);
        log.info("Delivery planned with ID: {} for order: {}, from: {}, to: {}",
                savedDelivery.getDeliveryId(),
                deliveryDto.getOrderId(),
                deliveryDto.getFromAddress().getStreet(),
                deliveryDto.getToAddress().getStreet());

        return deliveryMapper.toDto(savedDelivery);
    }

    private void validateDeliveryAddress(DeliveryDto deliveryDto) {
        if (deliveryDto.getFromAddress() == null || deliveryDto.getToAddress() == null) {
            log.error("Delivery addresses cannot be null");
            throw new IllegalArgumentException("Delivery addresses cannot be null");
        }

        if (deliveryDto.getFromAddress().getStreet() == null ||
                deliveryDto.getToAddress().getStreet() == null) {
            log.error("Street addresses cannot be null");
            throw new IllegalArgumentException("Street addresses cannot be null");
        }

        log.debug("Validated delivery addresses: from={}, to={}",
                deliveryDto.getFromAddress().getStreet(),
                deliveryDto.getToAddress().getStreet());
    }

    @Transactional
    public void deliveryFailed(UUID orderId) {
        log.info("Processing failed delivery for order: {}", orderId);

        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> {
                    log.error("Delivery not found for order: {}", orderId);
                    return new NoDeliveryFoundException();
                });

        delivery.setDeliveryState(DeliveryState.FAILED);
        deliveryRepository.save(delivery);

        try {
            orderServiceClient.deliveryFailed(orderId);
            log.info("Order {} status updated to DELIVERY_FAILED", orderId);
        } catch (Exception e) {
            log.error("Failed to update order status for order {}: {}", orderId, e.getMessage(), e);
            throw new RuntimeException("Failed to update order status", e);
        }

        log.info("Delivery for order {} marked as FAILED", orderId);
    }

    public BigDecimal deliveryCost(OrderDto orderDto) {
        log.info("Calculating delivery cost for order: {}", orderDto.getOrderId());

        Double weight = orderDto.getDeliveryWeight();
        Double volume = orderDto.getDeliveryVolume();
        Boolean fragile = orderDto.getFragile();

        if (weight == null || volume == null || fragile == null) {
            log.error("Order {} missing delivery parameters: weight={}, volume={}, fragile={}",
                    orderDto.getOrderId(), weight, volume, fragile);
            throw new IllegalArgumentException("Order missing delivery parameters (weight, volume, fragile)");
        }

        Delivery delivery = deliveryRepository.findByOrderId(orderDto.getOrderId()).orElse(null);

        if (delivery == null) {
            log.warn("Delivery not found for order {}, using default addresses", orderDto.getOrderId());
        }

        String fromStreet = delivery != null ? delivery.getFromStreet() : "ADDRESS_2";
        String toStreet = delivery != null ? delivery.getToStreet() : "Улица Пролетарская";

        BigDecimal cost = calculateDeliveryCost(fromStreet, toStreet, weight, volume, fragile);
        log.info("Calculated delivery cost for order {}: ${}", orderDto.getOrderId(), cost);

        return cost;
    }

    private BigDecimal calculateDeliveryCost(String fromStreet, String toStreet,
                                             Double weight, Double volume, Boolean fragile) {
        log.debug("Calculating cost: from={}, to={}, weight={}, volume={}, fragile={}",
                fromStreet, toStreet, weight, volume, fragile);

        BigDecimal baseCost = new BigDecimal("5.0");
        BigDecimal result = baseCost;

        // Address multiplier
        if (fromStreet.contains("ADDRESS_2")) {
            BigDecimal addressMultiplier = baseCost.multiply(new BigDecimal("2"));
            result = result.add(addressMultiplier);
            log.debug("Added address multiplier: {}", addressMultiplier);
        }

        // Fragile goods extra
        if (fragile != null && fragile) {
            BigDecimal fragileExtra = result.multiply(new BigDecimal("0.2"));
            result = result.add(fragileExtra);
            log.debug("Added fragile extra: {}", fragileExtra);
        }

        // Weight extra
        BigDecimal weightExtra = new BigDecimal(weight).multiply(new BigDecimal("0.3"));
        result = result.add(weightExtra);
        log.debug("Added weight extra: {}", weightExtra);

        // Volume extra
        BigDecimal volumeExtra = new BigDecimal(volume).multiply(new BigDecimal("0.2"));
        result = result.add(volumeExtra);
        log.debug("Added volume extra: {}", volumeExtra);

        // Different street extra
        if (!fromStreet.equals(toStreet)) {
            BigDecimal addressExtra = result.multiply(new BigDecimal("0.2"));
            result = result.add(addressExtra);
            log.debug("Added different street extra: {}", addressExtra);
        }

        BigDecimal finalCost = result.setScale(2, java.math.RoundingMode.HALF_UP);
        log.debug("Final delivery cost: {}", finalCost);

        return finalCost;
    }

    @Transactional
    public void deliverySuccessful(UUID orderId) {
        log.info("Processing successful delivery for order: {}", orderId);

        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> {
                    log.error("Delivery not found for order: {}", orderId);
                    return new NoDeliveryFoundException();
                });

        delivery.setDeliveryState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);

        try {
            orderServiceClient.deliverySuccess(orderId);
            log.info("Order {} status updated to DELIVERED", orderId);
        } catch (Exception e) {
            log.error("Failed to update order status for order {}: {}", orderId, e.getMessage(), e);
            // Don't throw exception here - delivery is already marked as DELIVERED
            // Just log the error for monitoring
        }

        log.info("Delivery for order {} marked as DELIVERED", orderId);
    }

    @Transactional
    public void deliveryPicked(UUID orderId) {
        log.info("Processing picked delivery for order: {}", orderId);

        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> {
                    log.error("Delivery not found for order: {}", orderId);
                    return new NoDeliveryFoundException();
                });

        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        deliveryRepository.save(delivery);

        try {
            ShippedToDeliveryRequest request = new ShippedToDeliveryRequest();
            request.setOrderId(orderId);
            request.setDeliveryId(delivery.getDeliveryId());

            warehouseDeliveryClient.shippedToDelivery(request);
            log.info("Order {} shipped to delivery {}", orderId, delivery.getDeliveryId());
        } catch (Exception e) {
            log.error("Failed to update warehouse for order {}: {}", orderId, e.getMessage(), e);
            // Revert delivery state
            delivery.setDeliveryState(DeliveryState.CREATED);
            deliveryRepository.save(delivery);
            throw new RuntimeException("Failed to update warehouse", e);
        }

        log.info("Delivery for order {} marked as IN_PROGRESS (picked)", orderId);
    }
}