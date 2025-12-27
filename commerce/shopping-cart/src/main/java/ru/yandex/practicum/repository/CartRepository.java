package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.entity.Cart;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface CartRepository extends JpaRepository<Cart, UUID> {

    Optional<Cart> findByUsernameAndStatus(String username, Cart.CartStatus status);

    // Новый метод для получения любой корзины (активной или деактивированной) с товарами
    @Query("SELECT c FROM Cart c LEFT JOIN FETCH c.items WHERE c.username = :username")
    Optional<Cart> findCartWithItemsByUsername(@Param("username") String username);

    // Метод для получения только активной корзины с товарами (используется для операций изменения)
    @Query("SELECT c FROM Cart c LEFT JOIN FETCH c.items WHERE c.username = :username AND c.status = 'ACTIVE'")
    Optional<Cart> findActiveCartWithItems(@Param("username") String username);
}