package ru.yandex.practicum.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NewProductInWarehouseRequest {
    private UUID productId;
    private DimensionDto dimension;
    private BigDecimal weight;
    private Boolean fragile;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DimensionDto {
        private BigDecimal width;
        private BigDecimal height;
        private BigDecimal depth;
    }
}