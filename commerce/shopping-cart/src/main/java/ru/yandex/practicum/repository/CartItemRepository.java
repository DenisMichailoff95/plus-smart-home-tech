package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.entity.CartItem;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface CartItemRepository extends JpaRepository<CartItem, UUID> {

    Optional<CartItem> findByCartShoppingCartIdAndProductId(UUID cartId, UUID productId);

    List<CartItem> findByCartShoppingCartId(UUID cartId);

    @Modifying
    @Query("DELETE FROM CartItem ci WHERE ci.cart.shoppingCartId = :cartId AND ci.productId IN :productIds")
    void deleteByCartIdAndProductIds(@Param("cartId") UUID cartId, @Param("productIds") List<UUID> productIds);

    @Modifying
    @Query("DELETE FROM CartItem ci WHERE ci.cart.shoppingCartId = :cartId")
    void deleteByCartId(@Param("cartId") UUID cartId);
}