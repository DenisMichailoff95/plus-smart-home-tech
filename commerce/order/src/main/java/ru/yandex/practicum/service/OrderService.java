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
        validateUsername(username);
        log.info("Getting orders for user: {}", username);

        List<Order> orders = orderRepository.findByUsername(username);
        return orders.stream()
                .map(orderMapper::toDto)
                .toList();
    }

    private void validateUsername(String username) {
        if (username == null || username.trim().isEmpty()) {
            throw new NotAuthorizedUserException();
        }
    }

    @Transactional
    public OrderDto complete(UUID orderId) {
        log.info("Completing order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        order.setState(OrderState.COMPLETED);

        Order savedOrder = orderRepository.save(order);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        log.info("Creating new order for user: {}, shopping cart: {}",
                request.getUsername(), request.getShoppingCart().getShoppingCartId());

        ShoppingCartDto shoppingCart = request.getShoppingCart();

        if (shoppingCart.getProducts() == null || shoppingCart.getProducts().isEmpty()) {
            throw new IllegalArgumentException("Shopping cart is empty");
        }

        if (request.getUsername() == null || request.getUsername().trim().isEmpty()) {
            throw new NotAuthorizedUserException();
        }

        Order order = new Order();
        order.setShoppingCartId(shoppingCart.getShoppingCartId());
        order.setUsername(request.getUsername());
        order.setState(OrderState.NEW);

        Order savedOrder = orderRepository.save(order);

        createOrderItems(savedOrder, shoppingCart.getProducts());

        Order orderWithItems = orderRepository.findByIdWithItems(savedOrder.getOrderId())
                .orElseThrow(() -> new NoOrderFoundException(savedOrder.getOrderId()));

        DeliveryDto deliveryDto = createDelivery(orderWithItems, request.getDeliveryAddress());
        orderWithItems.setDeliveryId(deliveryDto.getDeliveryId());

        OrderDto orderDto = orderMapper.toDto(orderWithItems);

        BigDecimal deliveryCost = deliveryServiceClient.deliveryCost(orderDto);
        BigDecimal productCost = paymentServiceClient.productCost(orderDto);
        BigDecimal totalCost = paymentServiceClient.getTotalCost(orderDto);

        orderWithItems.setDeliveryPrice(deliveryCost);
        orderWithItems.setProductPrice(productCost);
        orderWithItems.setTotalPrice(totalCost);

        Order updatedOrder = orderRepository.save(orderWithItems);

        log.info("Order created with ID: {} for user: {}",
                updatedOrder.getOrderId(), request.getUsername());

        return orderMapper.toDto(updatedOrder);
    }

    @Transactional
    public OrderDto productReturn(ProductReturnRequest request) {
        log.info("Processing return for order: {}", request.getOrderId());

        Order order = orderRepository.findByIdWithItems(request.getOrderId())
                .orElseThrow(() -> new NoOrderFoundException(request.getOrderId()));

        order.setState(OrderState.PRODUCT_RETURNED);

        if (request.getProducts() != null && !request.getProducts().isEmpty()) {
            try {
                warehouseServiceClient.acceptReturn(request.getProducts());
                log.info("Returned products to warehouse for order: {}", request.getOrderId());
            } catch (Exception e) {
                log.error("Failed to return products to warehouse for order {}: {}",
                        request.getOrderId(), e.getMessage());
            }
        }

        Order savedOrder = orderRepository.save(order);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto paymentSuccess(UUID orderId) {
        log.info("Payment success for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        order.setState(OrderState.PAID);

        Order savedOrder = orderRepository.save(order);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto payment(UUID orderId) {
        log.info("Payment process started for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        order.setState(OrderState.ON_PAYMENT);

        Order savedOrder = orderRepository.save(order);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto paymentFailed(UUID orderId) {
        log.info("Payment failed for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        order.setState(OrderState.PAYMENT_FAILED);

        Order savedOrder = orderRepository.save(order);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto delivery(UUID orderId) {
        log.info("Delivery for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        order.setState(OrderState.DELIVERED);

        Order savedOrder = orderRepository.save(order);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto deliveryFailed(UUID orderId) {
        log.info("Delivery failed for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        order.setState(OrderState.DELIVERY_FAILED);

        Order savedOrder = orderRepository.save(order);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto assembly(UUID orderId) {
        log.info("Assembling order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        AssemblyProductsForOrderRequest assemblyRequest = new AssemblyProductsForOrderRequest(
                convertItemsToMap(order.getItems()),
                orderId
        );

        warehouseServiceClient.assemblyProductForOrderFromShoppingCart(assemblyRequest);

        order.setState(OrderState.ASSEMBLED);

        Order savedOrder = orderRepository.save(order);
        return orderMapper.toDto(savedOrder);
    }

    @Transactional
    public OrderDto assemblyFailed(UUID orderId) {
        log.info("Assembly failed for order: {}", orderId);

        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new NoOrderFoundException(orderId));

        order.setState(OrderState.ASSEMBLY_FAILED);

        Order savedOrder = orderRepository.save(order);
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

        return orderMapper.toDto(savedOrder);
    }

    private DeliveryDto createDelivery(Order order, AddressDto deliveryAddress) {
        AddressDto warehouseAddress = warehouseServiceClient.getWarehouseAddress();

        DeliveryDto deliveryDto = new DeliveryDto();
        deliveryDto.setOrderId(order.getOrderId());
        deliveryDto.setFromAddress(warehouseAddress);
        deliveryDto.setToAddress(deliveryAddress);
        deliveryDto.setDeliveryState(ru.yandex.practicum.dto.delivery.DeliveryState.CREATED);

        return deliveryServiceClient.planDelivery(deliveryDto);
    }

    @Transactional
    protected void createOrderItems(Order order, Map<String, Integer> products) {
        List<OrderItem> orderItems = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : products.entrySet()) {
            try {
                OrderItem item = new OrderItem();
                item.setOrder(order);
                item.setProductId(UUID.fromString(entry.getKey()));
                item.setQuantity(entry.getValue());
                orderItems.add(item);
            } catch (IllegalArgumentException e) {
                log.error("Invalid UUID format: {}", entry.getKey());
                continue;
            }
        }

        orderItemRepository.saveAll(orderItems);
    }

    private Map<String, Integer> convertItemsToMap(List<OrderItem> items) {
        return items.stream()
                .collect(java.util.stream.Collectors.toMap(
                        item -> item.getProductId().toString(),
                        OrderItem::getQuantity
                ));
    }
}