package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.DeliveryServiceClient;
import ru.yandex.practicum.client.PaymentServiceClient;
import ru.yandex.practicum.client.WarehouseServiceClient;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.OrderState;
import ru.yandex.practicum.dto.order.ProductReturnRequest;
import ru.yandex.practicum.dto.shoppingcart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.AssemblyProductsForOrderRequest;
import ru.yandex.practicum.entity.Order;
import ru.yandex.practicum.entity.OrderItem;
import ru.yandex.practicum.exception.NoOrderFoundException;
import ru.yandex.practicum.exception.NotAuthorizedUserException;
import ru.yandex.practicum.mapper.OrderMapper;
import ru.yandex.practicum.repository.OrderItemRepository;
import ru.yandex.practicum.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final OrderMapper orderMapper;
    private final DeliveryServiceClient deliveryServiceClient;
    private final PaymentServiceClient paymentServiceClient;
    private final WarehouseServiceClient warehouseServiceClient;

    public List<OrderDto> getClientOrders(String username) {
        log.info("Getting orders for user: {}", username);
        validateUsername(username);

        List<Order> orders = orderRepository.findByUsername(username);
        log.info("Found {} orders for user: {}", orders.size(), username);

        return orders.stream()
                .map(orderMapper::toDto)
                .toList();
    }

    private void validateUsername(String username) {
        if (username == null || username.trim().isEmpty()) {
            log.error("Username validation failed: username is null or empty");
            throw new NotAuthorizedUserException();
        }
        log.debug("Username validation passed for: {}", username);
    }

    @Transactional
    public OrderDto complete(UUID orderId) {
        log.info("Completing order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> {
                    log.error("Order not found: {}", orderId);
                    return new NoOrderFoundException(orderId);
                });

        // Validate order can be completed
        if (order.getState() != OrderState.DELIVERED) {
            log.error("Order {} cannot be completed, current state: {}", orderId, order.getState());
            throw new IllegalStateException("Order must be DELIVERED before completion");
        }

        order.setState(OrderState.COMPLETED);
        Order savedOrder = orderRepository.save(order);

        log.info("Order {} marked as COMPLETED", orderId);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        log.info("Creating new order for user: {}, shopping cart: {}",
                request.getUsername(), request.getShoppingCart().getShoppingCartId());

        validateCreateOrderRequest(request);

        ShoppingCartDto shoppingCart = request.getShoppingCart();

        Order order = new Order();
        order.setShoppingCartId(shoppingCart.getShoppingCartId());
        order.setUsername(request.getUsername());
        order.setState(OrderState.NEW);

        Order savedOrder = orderRepository.save(order);
        log.info("Order created with ID: {}", savedOrder.getOrderId());

        createOrderItems(savedOrder, shoppingCart.getProducts());

        Order orderWithItems = orderRepository.findByIdWithItems(savedOrder.getOrderId())
                .orElseThrow(() -> new NoOrderFoundException(savedOrder.getOrderId()));

        // Create delivery
        DeliveryDto deliveryDto = createDelivery(orderWithItems, request.getDeliveryAddress());
        orderWithItems.setDeliveryId(deliveryDto.getDeliveryId());
        log.info("Delivery created for order {}: {}", savedOrder.getOrderId(), deliveryDto.getDeliveryId());

        // Calculate costs
        OrderDto orderDto = orderMapper.toDto(orderWithItems);

        BigDecimal deliveryCost = deliveryServiceClient.deliveryCost(orderDto);
        BigDecimal productCost = paymentServiceClient.productCost(orderDto);
        BigDecimal totalCost = paymentServiceClient.getTotalCost(orderDto);

        orderWithItems.setDeliveryPrice(deliveryCost);
        orderWithItems.setProductPrice(productCost);
        orderWithItems.setTotalPrice(totalCost);

        Order updatedOrder = orderRepository.save(orderWithItems);

        log.info("Order {} finalized - Delivery: ${}, Products: ${}, Total: ${}",
                updatedOrder.getOrderId(), deliveryCost, productCost, totalCost);

        return orderMapper.toDto(updatedOrder);
    }

    private void validateCreateOrderRequest(CreateNewOrderRequest request) {
        if (request == null) {
            log.error("Create order request is null");
            throw new IllegalArgumentException("Request cannot be null");
        }

        if (request.getUsername() == null || request.getUsername().trim().isEmpty()) {
            log.error("Username is required");
            throw new NotAuthorizedUserException();
        }

        if (request.getShoppingCart() == null) {
            log.error("Shopping cart is required");
            throw new IllegalArgumentException("Shopping cart cannot be null");
        }

        if (request.getShoppingCart().getProducts() == null ||
                request.getShoppingCart().getProducts().isEmpty()) {
            log.error("Shopping cart is empty");
            throw new IllegalArgumentException("Shopping cart is empty");
        }

        if (request.getDeliveryAddress() == null) {
            log.error("Delivery address is required");
            throw new IllegalArgumentException("Delivery address cannot be null");
        }

        log.debug("Create order request validated successfully");
    }

    @Transactional
    public OrderDto productReturn(ProductReturnRequest request) {
        log.info("Processing return for order: {}", request.getOrderId());

        Order order = orderRepository.findByIdWithItems(request.getOrderId())
                .orElseThrow(() -> new NoOrderFoundException(request.getOrderId()));

        // Validate return
        if (order.getState() != OrderState.DELIVERED && order.getState() != OrderState.COMPLETED) {
            log.error("Order {} cannot be returned, current state: {}", request.getOrderId(), order.getState());
            throw new IllegalStateException("Only DELIVERED or COMPLETED orders can be returned");
        }

        order.setState(OrderState.PRODUCT_RETURNED);

        if (request.getProducts() != null && !request.getProducts().isEmpty()) {
            try {
                warehouseServiceClient.acceptReturn(request.getProducts());
                log.info("Returned {} products to warehouse for order: {}",
                        request.getProducts().size(), request.getOrderId());
            } catch (Exception e) {
                log.error("Failed to return products to warehouse for order {}: {}",
                        request.getOrderId(), e.getMessage(), e);
                // Don't throw - mark order as returned even if warehouse update fails
            }
        }

        Order savedOrder = orderRepository.save(order);
        log.info("Order {} marked as PRODUCT_RETURNED", request.getOrderId());
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto paymentSuccess(UUID orderId) {
        log.info("Payment success for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        // Validate payment transition
        if (order.getState() != OrderState.ON_PAYMENT) {
            log.warn("Order {} payment success received but order is in state: {}",
                    orderId, order.getState());
        }

        order.setState(OrderState.PAID);
        Order savedOrder = orderRepository.save(order);

        log.info("Order {} payment marked as PAID", orderId);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto payment(UUID orderId) {
        log.info("Payment process started for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        // Validate order can be paid
        if (order.getState() != OrderState.NEW) {
            log.error("Order {} cannot be paid, current state: {}", orderId, order.getState());
            throw new IllegalStateException("Only NEW orders can be paid");
        }

        order.setState(OrderState.ON_PAYMENT);
        Order savedOrder = orderRepository.save(order);

        log.info("Order {} marked as ON_PAYMENT", orderId);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto paymentFailed(UUID orderId) {
        log.info("Payment failed for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        order.setState(OrderState.PAYMENT_FAILED);
        Order savedOrder = orderRepository.save(order);

        log.info("Order {} marked as PAYMENT_FAILED", orderId);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto delivery(UUID orderId) {
        log.info("Delivery for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        // Validate delivery transition
        if (order.getState() != OrderState.ASSEMBLED && order.getState() != OrderState.PAID) {
            log.warn("Order {} delivery received but order is in state: {}",
                    orderId, order.getState());
        }

        order.setState(OrderState.DELIVERED);
        Order savedOrder = orderRepository.save(order);

        log.info("Order {} marked as DELIVERED", orderId);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto deliveryFailed(UUID orderId) {
        log.info("Delivery failed for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        order.setState(OrderState.DELIVERY_FAILED);
        Order savedOrder = orderRepository.save(order);

        log.info("Order {} marked as DELIVERY_FAILED", orderId);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto assembly(UUID orderId) {
        log.info("Assembling order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        // Validate order can be assembled
        if (order.getState() != OrderState.PAID) {
            log.error("Order {} cannot be assembled, current state: {}", orderId, order.getState());
            throw new IllegalStateException("Only PAID orders can be assembled");
        }

        AssemblyProductsForOrderRequest assemblyRequest = new AssemblyProductsForOrderRequest(
                convertItemsToMap(order.getItems()),
                orderId
        );

        try {
            warehouseServiceClient.assemblyProductForOrderFromShoppingCart(assemblyRequest);
            log.info("Products assembled for order: {}", orderId);
        } catch (Exception e) {
            log.error("Failed to assemble products for order {}: {}", orderId, e.getMessage(), e);
            throw new RuntimeException("Failed to assemble order", e);
        }

        order.setState(OrderState.ASSEMBLED);
        Order savedOrder = orderRepository.save(order);

        log.info("Order {} marked as ASSEMBLED", orderId);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto assemblyFailed(UUID orderId) {
        log.info("Assembly failed for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        order.setState(OrderState.ASSEMBLY_FAILED);
        Order savedOrder = orderRepository.save(order);

        log.info("Order {} marked as ASSEMBLY_FAILED", orderId);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto calculateTotalCost(UUID orderId) {
        log.info("Calculating total cost for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        OrderDto orderDto = orderMapper.toDto(order);
        BigDecimal totalCost = paymentServiceClient.getTotalCost(orderDto);

        order.setTotalPrice(totalCost);
        Order savedOrder = orderRepository.save(order);

        log.info("Total cost calculated for order {}: ${}", orderId, totalCost);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto calculateDeliveryCost(UUID orderId) {
        log.info("Calculating delivery cost for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        OrderDto orderDto = orderMapper.toDto(order);
        BigDecimal deliveryCost = deliveryServiceClient.deliveryCost(orderDto);

        order.setDeliveryPrice(deliveryCost);
        Order savedOrder = orderRepository.save(order);

        log.info("Delivery cost calculated for order {}: ${}", orderId, deliveryCost);
        return orderMapper.toDto(savedOrder);
    }

    private DeliveryDto createDelivery(Order order, AddressDto deliveryAddress) {
        log.debug("Creating delivery for order: {}", order.getOrderId());

        AddressDto warehouseAddress = warehouseServiceClient.getWarehouseAddress();
        log.debug("Warehouse address: {}", warehouseAddress.getStreet());

        DeliveryDto deliveryDto = new DeliveryDto();
        deliveryDto.setOrderId(order.getOrderId());
        deliveryDto.setFromAddress(warehouseAddress);
        deliveryDto.setToAddress(deliveryAddress);
        deliveryDto.setDeliveryState(ru.yandex.practicum.dto.delivery.DeliveryState.CREATED);

        return deliveryServiceClient.planDelivery(deliveryDto);
    }

    @Transactional
    protected void createOrderItems(Order order, Map<String, Integer> products) {
        log.debug("Creating order items for order: {}", order.getOrderId());

        List<OrderItem> orderItems = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : products.entrySet()) {
            try {
                OrderItem item = new OrderItem();
                item.setOrder(order);
                item.setProductId(UUID.fromString(entry.getKey()));
                item.setQuantity(entry.getValue());
                orderItems.add(item);
                log.debug("Added order item: product={}, quantity={}",
                        entry.getKey(), entry.getValue());
            } catch (IllegalArgumentException e) {
                log.error("Invalid UUID format: {}, skipping", entry.getKey());
                continue;
            }
        }

        orderItemRepository.saveAll(orderItems);
        log.info("Created {} order items for order: {}", orderItems.size(), order.getOrderId());
    }

    private Map<String, Integer> convertItemsToMap(List<OrderItem> items) {
        log.debug("Converting {} order items to map", items.size());
        return items.stream()
                .collect(java.util.stream.Collectors.toMap(
                        item -> item.getProductId().toString(),
                        OrderItem::getQuantity
                ));
    }
}