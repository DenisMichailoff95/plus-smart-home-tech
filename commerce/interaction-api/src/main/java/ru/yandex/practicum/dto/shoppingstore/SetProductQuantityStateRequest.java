package ru.yandex.practicum.dto.shoppingstore;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SetProductQuantityStateRequest {
    private UUID productId;
    private QuantityState quantityState;
}