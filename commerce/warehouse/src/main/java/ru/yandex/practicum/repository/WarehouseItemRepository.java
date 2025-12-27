package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.entity.WarehouseItem;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface WarehouseItemRepository extends JpaRepository<WarehouseItem, UUID> {

    Optional<WarehouseItem> findByProductId(UUID productId);

    List<WarehouseItem> findByProductIdIn(List<UUID> productIds);

    @Query("SELECT wi FROM WarehouseItem wi WHERE wi.productId IN :productIds AND wi.quantity > 0")
    List<WarehouseItem> findAvailableItemsByProductIds(@Param("productIds") List<UUID> productIds);

    boolean existsByProductId(UUID productId);

    @Modifying
    @Query("UPDATE WarehouseItem wi SET wi.quantity = wi.quantity + :quantity WHERE wi.productId = :productId")
    void updateQuantity(@Param("productId") UUID productId, @Param("quantity") Integer quantity);
}