package ru.yandex.practicum.mapper;

import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;
import org.mapstruct.Named;
import ru.yandex.practicum.dto.shoppingcart.ShoppingCartDto;
import ru.yandex.practicum.entity.Cart;
import ru.yandex.practicum.entity.CartItem;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Mapper(componentModel = "spring")
public interface ShoppingCartMapper {

    @Mapping(target = "products", source = "items", qualifiedByName = "mapItemsToProducts")
    @Mapping(target = "shoppingCartId", source = "shoppingCartId")
    ShoppingCartDto toDto(Cart cart);

    @Mapping(target = "items", source = "products", qualifiedByName = "mapProductsToItems")
    @Mapping(target = "shoppingCartId", source = "shoppingCartId")
    @Mapping(target = "username", ignore = true)
    @Mapping(target = "status", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    Cart toEntity(ShoppingCartDto dto);

    @AfterMapping
    default void setCartReferences(@MappingTarget Cart cart) {
        if (cart != null && cart.getItems() != null) {
            for (CartItem item : cart.getItems()) {
                item.setCart(cart);
            }
        }
    }

    default Cart toEntity(ShoppingCartDto dto, String username) {
        Cart cart = toEntity(dto);
        if (cart != null) {
            cart.setUsername(username);
        }
        return cart;
    }

    @Named("mapItemsToProducts")
    default Map<String, Integer> mapItemsToProducts(java.util.List<CartItem> items) {
        if (items == null) {
            return new HashMap<>();
        }

        return items.stream()
                .filter(item -> item != null && item.getProductId() != null)
                .collect(Collectors.toMap(
                        item -> item.getProductId().toString(),
                        CartItem::getQuantity
                ));
    }

    @Named("mapProductsToItems")
    default java.util.List<CartItem> mapProductsToItems(Map<String, Integer> products) {
        if (products == null) {
            return null;
        }

        return products.entrySet().stream()
                .map(entry -> {
                    try {
                        CartItem item = new CartItem();
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