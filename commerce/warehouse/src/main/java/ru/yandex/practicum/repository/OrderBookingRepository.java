package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.entity.OrderBooking;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface OrderBookingRepository extends JpaRepository<OrderBooking, UUID> {

    Optional<OrderBooking> findByOrderId(UUID orderId);

    @Modifying
    @Query("UPDATE OrderBooking ob SET ob.deliveryId = :deliveryId WHERE ob.orderId = :orderId")
    void updateDeliveryId(@Param("orderId") UUID orderId, @Param("deliveryId") UUID deliveryId);

    boolean existsByOrderId(UUID orderId);
}