package ru.yandex.practicum.dto.order;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.dto.shoppingcart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateNewOrderRequest {
    private String username;  // ДОБАВЛЕНО: для связи заказа с пользователем (расширение спецификации)
    private ShoppingCartDto shoppingCart;
    private AddressDto deliveryAddress;
}