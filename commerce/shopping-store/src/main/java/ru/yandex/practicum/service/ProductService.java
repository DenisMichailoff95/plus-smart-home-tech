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
        log.info("[ProductService] Fetching products by category: {}", category);
        log.debug("[ProductService] Pageable: page={}, size={}, sort={}",
                pageable.getPageNumber(), pageable.getPageSize(), pageable.getSort());

        long startTime = System.currentTimeMillis();
        try {
            var productCategory = ru.yandex.practicum.dto.shoppingstore.ProductCategory.valueOf(category);
            Page<Product> products = productRepository.findByProductCategoryAndProductState(
                    productCategory,
                    ru.yandex.practicum.dto.shoppingstore.ProductState.ACTIVE,
                    pageable
            );

            long duration = System.currentTimeMillis() - startTime;
            log.info("[ProductService] Found {} products in category '{}' in {} ms",
                    products.getTotalElements(), category, duration);
            log.debug("[ProductService] Page info: {} of {} pages",
                    products.getNumber() + 1, products.getTotalPages());

            return products.map(productMapper::toDto);
        } catch (IllegalArgumentException e) {
            log.error("[ProductService] Invalid category: {}", category, e);
            throw e;
        } catch (Exception e) {
            log.error("[ProductService] Error fetching products for category: {}", category, e);
            throw e;
        }
    }

    public ProductDto getProduct(UUID productId) {
        log.info("[ProductService] Getting product by ID: {}", productId);

        long startTime = System.currentTimeMillis();
        try {
            Product product = productRepository.findById(productId)
                    .orElseThrow(() -> {
                        log.warn("[ProductService] Product not found with ID: {}", productId);
                        return new ProductNotFoundException(productId);
                    });

            ProductDto result = productMapper.toDto(product);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[ProductService] Product retrieved successfully in {} ms", duration);
            log.debug("[ProductService] Product details: name={}, price={}, state={}",
                    product.getProductName(), product.getPrice(), product.getProductState());

            return result;
        } catch (Exception e) {
            log.error("[ProductService] Error getting product with ID: {}", productId, e);
            throw e;
        }
    }

    @Transactional
    public ProductDto createProduct(ProductDto productDto) {
        log.info("[ProductService] Creating new product: {}", productDto.getProductName());
        log.debug("[ProductService] Product DTO: {}", productDto);

        long startTime = System.currentTimeMillis();
        try {
            if (productDto.getPrice() == null || productDto.getPrice().compareTo(java.math.BigDecimal.ZERO) <= 0) {
                log.error("[ProductService] Invalid product price: {}", productDto.getPrice());
                throw new IllegalArgumentException("Product price must be positive");
            }

            Product product = productMapper.toEntity(productDto);
            if (product.getProductId() == null) {
                UUID newId = UUID.randomUUID();
                product.setProductId(newId);
                log.debug("[ProductService] Generated new product ID: {}", newId);
            }

            Product savedProduct = productRepository.save(product);
            ProductDto result = productMapper.toDto(savedProduct);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[ProductService] Product created successfully with ID: {} in {} ms",
                    savedProduct.getProductId(), duration);

            return result;
        } catch (Exception e) {
            log.error("[ProductService] Error creating product: {}", productDto.getProductName(), e);
            throw e;
        }
    }

    @Transactional
    public ProductDto updateProduct(ProductDto productDto) {
        log.info("[ProductService] Updating product: {}", productDto.getProductId());
        log.debug("[ProductService] Update data: {}", productDto);

        long startTime = System.currentTimeMillis();
        try {
            Product existingProduct = productRepository.findById(productDto.getProductId())
                    .orElseThrow(() -> {
                        log.warn("[ProductService] Product not found for update: {}", productDto.getProductId());
                        return new ProductNotFoundException(productDto.getProductId());
                    });

            log.debug("[ProductService] Existing product before update: name={}, price={}",
                    existingProduct.getProductName(), existingProduct.getPrice());

            productMapper.updateEntityFromDto(productDto, existingProduct);
            Product updatedProduct = productRepository.save(existingProduct);
            ProductDto result = productMapper.toDto(updatedProduct);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[ProductService] Product updated successfully in {} ms", duration);
            log.debug("[ProductService] Product after update: name={}, price={}",
                    updatedProduct.getProductName(), updatedProduct.getPrice());

            return result;
        } catch (Exception e) {
            log.error("[ProductService] Error updating product: {}", productDto.getProductId(), e);
            throw e;
        }
    }

    @Transactional
    public Boolean removeProductFromStore(UUID productId) {
        log.info("[ProductService] Removing product from store: {}", productId);

        long startTime = System.currentTimeMillis();
        try {
            Product product = productRepository.findById(productId)
                    .orElseThrow(() -> {
                        log.warn("[ProductService] Product not found for removal: {}", productId);
                        return new ProductNotFoundException(productId);
                    });

            log.debug("[ProductService] Product current state: {}", product.getProductState());
            product.setProductState(ru.yandex.practicum.dto.shoppingstore.ProductState.DEACTIVATE);
            productRepository.save(product);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[ProductService] Product deactivated successfully in {} ms", duration);

            return true;
        } catch (Exception e) {
            log.error("[ProductService] Error removing product: {}", productId, e);
            throw e;
        }
    }

    @Transactional
    public Boolean setQuantityState(SetProductQuantityStateRequest request) {
        log.info("[ProductService] Setting quantity state for product: {}", request.getProductId());
        log.debug("[ProductService] New quantity state: {}", request.getQuantityState());

        long startTime = System.currentTimeMillis();
        try {
            Product product = productRepository.findById(request.getProductId())
                    .orElseThrow(() -> {
                        log.warn("[ProductService] Product not found for quantity state update: {}",
                                request.getProductId());
                        return new ProductNotFoundException(request.getProductId());
                    });

            log.debug("[ProductService] Previous quantity state: {}", product.getQuantityState());
            product.setQuantityState(request.getQuantityState());
            productRepository.save(product);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[ProductService] Quantity state updated successfully in {} ms", duration);

            return true;
        } catch (Exception e) {
            log.error("[ProductService] Error setting quantity state for product: {}",
                    request.getProductId(), e);
            throw e;
        }
    }
}