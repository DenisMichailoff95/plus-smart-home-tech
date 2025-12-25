package ru.yandex.practicum.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.entity.Product;

import java.util.List;
import java.util.UUID;

@Repository
public interface ProductRepository extends JpaRepository<Product, UUID> {

    // Этот метод должен искать активные продукты по категории
    Page<Product> findByProductCategoryAndProductState(
            ru.yandex.practicum.dto.shoppingstore.ProductCategory productCategory,
            ru.yandex.practicum.dto.shoppingstore.ProductState productState,
            Pageable pageable
    );

    List<Product> findByProductState(ru.yandex.practicum.dto.shoppingstore.ProductState productState);

    @Query("SELECT p FROM Product p WHERE p.productCategory = :category AND p.productState = 'ACTIVE'")
    List<Product> findActiveProductsByCategory(@Param("category") ru.yandex.practicum.dto.shoppingstore.ProductCategory category);

    boolean existsByProductIdAndProductState(UUID productId, ru.yandex.practicum.dto.shoppingstore.ProductState productState);
}