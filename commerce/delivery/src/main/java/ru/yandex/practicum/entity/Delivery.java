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
import ru.yandex.practicum.dto.delivery.DeliveryState;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "deliveries", schema = "delivery_schema")
@Getter
@Setter
public class Delivery {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID deliveryId;

    @Column(nullable = false)
    private UUID orderId;

    @Column(nullable = false)
    private String fromCountry;

    @Column(nullable = false)
    private String fromCity;

    @Column(nullable = false)
    private String fromStreet;

    @Column(nullable = false)
    private String fromHouse;

    @Column(nullable = false)
    private String fromFlat;

    @Column(nullable = false)
    private String toCountry;

    @Column(nullable = false)
    private String toCity;

    @Column(nullable = false)
    private String toStreet;

    @Column(nullable = false)
    private String toHouse;

    @Column(nullable = false)
    private String toFlat;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DeliveryState deliveryState = DeliveryState.CREATED;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column
    private LocalDateTime updatedAt = LocalDateTime.now();

    @PreUpdate
    public void preUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}