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

    @Query("SELECT c FROM Cart c LEFT JOIN FETCH c.items WHERE c.username = :username AND c.status = 'ACTIVE'")
    Optional<Cart> findActiveCartWithItems(@Param("username") String username);
}