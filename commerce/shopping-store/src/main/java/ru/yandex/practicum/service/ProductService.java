package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.shoppingstore.ProductDto;
import ru.yandex.practicum.dto.shoppingstore.SetProductQuantityStateRequest;
import ru.yandex.practicum.entity.Product;
import ru.yandex.practicum.exception.ProductNotFoundException;
import ru.yandex.practicum.mapper.ProductMapper;
import ru.yandex.practicum.repository.ProductRepository;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductService {

    private final ProductRepository productRepository;
    private final ProductMapper productMapper;


    public Page<ProductDto> getProductsByCategory(String category, Pageable pageable) {
        log.info("Getting products by category: {}, pageable: {}", category, pageable);

        // Проверяем валидность категории
        try {
            var productCategory = ru.yandex.practicum.dto.shoppingstore.ProductCategory.valueOf(category.toUpperCase());

            Page<Product> products = productRepository.findByProductCategoryAndProductState(
                    productCategory,
                    ru.yandex.practicum.dto.shoppingstore.ProductState.ACTIVE,
                    pageable
            );

            log.debug("Found {} products for category: {}", products.getTotalElements(), category);
            return products.map(productMapper::toDto);

        } catch (IllegalArgumentException e) {
            log.error("Invalid category: {}", category);
            // Возвращаем пустую страницу для невалидной категории
            return Page.empty(pageable);
        }
    }

    public ProductDto getProduct(UUID productId) {
        log.info("Getting product by id: {}", productId);

        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));

        return productMapper.toDto(product);
    }

    @Transactional
    public ProductDto createProduct(ProductDto productDto) {
        log.info("Creating product: {}", productDto.getProductName());

        // Убедимся, что продукт создается с правильным именем
        if (productDto.getProductName() == null || productDto.getProductName().trim().isEmpty()) {
            log.error("Product name cannot be null or empty");
            throw new IllegalArgumentException("Product name cannot be null or empty");
        }

        Product product = productMapper.toEntity(productDto);
        if (product.getProductId() == null) {
            product.setProductId(UUID.randomUUID());
        }

        // Убедимся, что продукт создается активным
        if (product.getProductState() == null) {
            product.setProductState(ru.yandex.practicum.dto.shoppingstore.ProductState.ACTIVE);
        }

        Product savedProduct = productRepository.save(product);
        log.info("Product created successfully: id={}, name={}",
                savedProduct.getProductId(), savedProduct.getProductName());

        return productMapper.toDto(savedProduct);
    }

    @Transactional
    public ProductDto updateProduct(ProductDto productDto) {
        log.info("Updating product: {}", productDto.getProductId());

        Product existingProduct = productRepository.findById(productDto.getProductId())
                .orElseThrow(() -> new ProductNotFoundException(productDto.getProductId()));

        // Сохраняем оригинальное имя для логов
        String originalName = existingProduct.getProductName();

        productMapper.updateEntityFromDto(productDto, existingProduct);
        Product updatedProduct = productRepository.save(existingProduct);

        log.info("Product updated: id={}, original name={}, new name={}",
                productDto.getProductId(), originalName, updatedProduct.getProductName());

        return productMapper.toDto(updatedProduct);
    }

    @Transactional
    public Boolean removeProductFromStore(UUID productId) {
        log.info("Removing product from store: {}", productId);

        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));

        product.setProductState(ru.yandex.practicum.dto.shoppingstore.ProductState.DEACTIVATE);
        productRepository.save(product);

        log.info("Product deactivated: {}", productId);
        return true;
    }

    @Transactional
    public Boolean setQuantityState(SetProductQuantityStateRequest request) {
        log.info("Setting quantity state for product: {}", request.getProductId());

        Product product = productRepository.findById(request.getProductId())
                .orElseThrow(() -> new ProductNotFoundException(request.getProductId()));

        product.setQuantityState(request.getQuantityState());
        productRepository.save(product);

        log.info("Quantity state updated for product: {} to {}",
                request.getProductId(), request.getQuantityState());
        return true;
    }
}