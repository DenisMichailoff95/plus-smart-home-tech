package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.entity.OrderItem;

import java.util.List;
import java.util.UUID;

@Repository
public interface OrderItemRepository extends JpaRepository<OrderItem, UUID> {

    List<OrderItem> findByOrderOrderId(UUID orderId);

    @Modifying
    @Query("DELETE FROM OrderItem oi WHERE oi.order.orderId = :orderId")
    void deleteByOrderId(@Param("orderId") UUID orderId);

    @Modifying
    @Query("DELETE FROM OrderItem oi WHERE oi.order.orderId = :orderId AND oi.productId IN :productIds")
    void deleteByOrderIdAndProductIds(@Param("orderId") UUID orderId,
                                      @Param("productIds") List<UUID> productIds);
}