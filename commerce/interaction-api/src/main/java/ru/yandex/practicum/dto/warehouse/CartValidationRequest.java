package ru.yandex.practicum.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CartValidationRequest {
    private Map<String, Integer> products;
}