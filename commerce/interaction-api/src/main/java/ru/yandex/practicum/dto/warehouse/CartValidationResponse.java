package ru.yandex.practicum.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CartValidationResponse {
    private Boolean isValid;
    private Map<UUID, String> validationErrors;
}