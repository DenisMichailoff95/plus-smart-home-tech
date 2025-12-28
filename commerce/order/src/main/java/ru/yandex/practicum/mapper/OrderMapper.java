package ru.yandex.practicum.mapper;

import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;
import org.mapstruct.Named;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.entity.Order;
import ru.yandex.practicum.entity.OrderItem;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Mapper(componentModel = "spring")
public interface OrderMapper {

    @Mapping(target = "products", source = "items", qualifiedByName = "mapItemsToProducts")
    @Mapping(target = "orderId", source = "orderId")
    @Mapping(target = "shoppingCartId", source = "shoppingCartId")
    @Mapping(target = "username", source = "username")
    @Mapping(target = "paymentId", source = "paymentId")
    @Mapping(target = "deliveryId", source = "deliveryId")
    @Mapping(target = "state", source = "state")
    @Mapping(target = "deliveryWeight", source = "deliveryWeight")
    @Mapping(target = "deliveryVolume", source = "deliveryVolume")
    @Mapping(target = "fragile", source = "fragile")
    @Mapping(target = "totalPrice", source = "totalPrice")
    @Mapping(target = "deliveryPrice", source = "deliveryPrice")
    @Mapping(target = "productPrice", source = "productPrice")
    OrderDto toDto(Order order);

    @Mapping(target = "items", source = "products", qualifiedByName = "mapProductsToItems")
    @Mapping(target = "orderId", source = "orderId")
    @Mapping(target = "shoppingCartId", source = "shoppingCartId")
    @Mapping(target = "username", source = "username")
    @Mapping(target = "paymentId", source = "paymentId")
    @Mapping(target = "deliveryId", source = "deliveryId")
    @Mapping(target = "state", source = "state")
    @Mapping(target = "deliveryWeight", source = "deliveryWeight")
    @Mapping(target = "deliveryVolume", source = "deliveryVolume")
    @Mapping(target = "fragile", source = "fragile")
    @Mapping(target = "totalPrice", source = "totalPrice")
    @Mapping(target = "deliveryPrice", source = "deliveryPrice")
    @Mapping(target = "productPrice", source = "productPrice")
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    Order toEntity(OrderDto dto);

    default Order toEntityFromRequest(CreateNewOrderRequest request) {
        if (request == null) {
            return null;
        }

        Order order = new Order();
        order.setShoppingCartId(request.getShoppingCart().getShoppingCartId());
        order.setUsername(request.getUsername());
        order.setState(ru.yandex.practicum.dto.order.OrderState.NEW);

        if (request.getShoppingCart() != null && request.getShoppingCart().getProducts() != null) {
            order.setItems(mapProductsToItems(request.getShoppingCart().getProducts()));
        }

        return order;
    }

    @AfterMapping
    default void setOrderReferences(@MappingTarget Order order) {
        if (order != null && order.getItems() != null) {
            for (OrderItem item : order.getItems()) {
                item.setOrder(order);
            }
        }
    }

    @Named("mapItemsToProducts")
    default Map<String, Integer> mapItemsToProducts(java.util.List<OrderItem> items) {
        if (items == null) {
            return new HashMap<>();
        }

        return items.stream()
                .filter(item -> item != null && item.getProductId() != null)
                .collect(Collectors.toMap(
                        item -> item.getProductId().toString(),
                        OrderItem::getQuantity
                ));
    }

    @Named("mapProductsToItems")
    default java.util.List<OrderItem> mapProductsToItems(Map<String, Integer> products) {
        if (products == null) {
            return null;
        }

        return products.entrySet().stream()
                .map(entry -> {
                    try {
                        OrderItem item = new OrderItem();
                        item.setProductId(UUID.fromString(entry.getKey()));
                        item.setQuantity(entry.getValue());
                        return item;
                    } catch (IllegalArgumentException e) {
                        return null;
                    }
                })
                .filter(item -> item != null)
                .collect(Collectors.toList());
    }
}