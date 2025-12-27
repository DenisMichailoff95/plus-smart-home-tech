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
            throw new IllegalArgumentException("Delivery already exists for order: " + deliveryDto.getOrderId());
        }

        Delivery delivery = deliveryMapper.toEntity(deliveryDto);
        delivery.setDeliveryState(DeliveryState.CREATED);

        Delivery savedDelivery = deliveryRepository.save(delivery);
        log.info("Delivery planned with ID: {} for order: {}",
                savedDelivery.getDeliveryId(), deliveryDto.getOrderId());

        return deliveryMapper.toDto(savedDelivery);
    }

    @Transactional
    public void deliveryFailed(UUID orderId) {
        log.info("Processing failed delivery for order: {}", orderId);

        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException());

        delivery.setDeliveryState(DeliveryState.FAILED);
        deliveryRepository.save(delivery);

        orderServiceClient.deliveryFailed(orderId);

        log.info("Delivery for order {} marked as FAILED", orderId);
    }

    public BigDecimal deliveryCost(OrderDto orderDto) {
        log.info("Calculating delivery cost for order: {}", orderDto.getOrderId());

        Double weight = orderDto.getDeliveryWeight();
        Double volume = orderDto.getDeliveryVolume();
        Boolean fragile = orderDto.getFragile();

        if (weight == null || volume == null || fragile == null) {
            throw new IllegalArgumentException("Order missing delivery parameters (weight, volume, fragile)");
        }

        Delivery delivery = deliveryRepository.findByOrderId(orderDto.getOrderId()).orElse(null);

        String fromStreet = "ADDRESS_2";
        String toStreet = delivery != null ? delivery.getToStreet() : "Улица Пролетарская";

        if (delivery != null) {
            fromStreet = delivery.getFromStreet();
            toStreet = delivery.getToStreet();
        }

        return calculateDeliveryCost(fromStreet, toStreet, weight, volume, fragile);
    }

    private BigDecimal calculateDeliveryCost(String fromStreet, String toStreet,
                                             Double weight, Double volume, Boolean fragile) {

        BigDecimal baseCost = new BigDecimal("5.0");
        BigDecimal result = baseCost;

        if (fromStreet.contains("ADDRESS_2")) {
            result = result.add(baseCost.multiply(new BigDecimal("2")));
        }

        if (fragile != null && fragile) {
            BigDecimal fragileExtra = result.multiply(new BigDecimal("0.2"));
            result = result.add(fragileExtra);
        }

        BigDecimal weightExtra = new BigDecimal(weight).multiply(new BigDecimal("0.3"));
        result = result.add(weightExtra);

        BigDecimal volumeExtra = new BigDecimal(volume).multiply(new BigDecimal("0.2"));
        result = result.add(volumeExtra);

        if (!fromStreet.equals(toStreet)) {
            BigDecimal addressExtra = result.multiply(new BigDecimal("0.2"));
            result = result.add(addressExtra);
        }

        return result.setScale(2, java.math.RoundingMode.HALF_UP);
    }

    @Transactional
    public void deliverySuccessful(UUID orderId) {
        log.info("Processing successful delivery for order: {}", orderId);

        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException());

        delivery.setDeliveryState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);

        try {
            orderServiceClient.deliverySuccess(orderId);
        } catch (Exception e) {
            log.error("Failed to update order status for order {}: {}", orderId, e.getMessage());
        }

        log.info("Delivery for order {} marked as DELIVERED", orderId);
    }

    @Transactional
    public void deliveryPicked(UUID orderId) {
        log.info("Processing picked delivery for order: {}", orderId);

        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException());

        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        deliveryRepository.save(delivery);

        try {
            ShippedToDeliveryRequest request = new ShippedToDeliveryRequest();
            request.setOrderId(orderId);
            request.setDeliveryId(delivery.getDeliveryId());
            warehouseDeliveryClient.shippedToDelivery(request);
            log.info("Order {} shipped to delivery {}", orderId, delivery.getDeliveryId());
        } catch (Exception e) {
            log.error("Failed to update warehouse for order {}: {}", orderId, e.getMessage());
        }

        log.info("Delivery for order {} marked as IN_PROGRESS (picked)", orderId);
    }
}