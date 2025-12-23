package ru.yandex.practicum.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.dto.shoppingstore.ProductCategory;
import ru.yandex.practicum.dto.shoppingstore.ProductState;
import ru.yandex.practicum.dto.shoppingstore.QuantityState;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "products", schema = "shopping_store")
@Getter
@Setter
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID productId;

    @Column(nullable = false)
    private String productName;

    @Column(nullable = false, length = 1000)
    private String description;

    private String imageSrc;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private QuantityState quantityState = QuantityState.ENDED;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ProductState productState = ProductState.ACTIVE;

    @Enumerated(EnumType.STRING)
    private ProductCategory productCategory;

    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal price;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    private LocalDateTime updatedAt = LocalDateTime.now();

    @PreUpdate
    public void preUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}