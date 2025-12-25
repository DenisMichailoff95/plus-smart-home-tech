package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.shoppingcart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.shoppingcart.ShoppingCartDto;
import ru.yandex.practicum.entity.Cart;
import ru.yandex.practicum.entity.CartItem;
import ru.yandex.practicum.exception.CartNotFoundException;
import ru.yandex.practicum.exception.NotAuthorizedUserException;
import ru.yandex.practicum.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.exception.InsufficientStockException;
import ru.yandex.practicum.exception.CartDeactivatedException;
import ru.yandex.practicum.mapper.ShoppingCartMapper;
import ru.yandex.practicum.repository.CartRepository;
import ru.yandex.practicum.repository.CartItemRepository;
import ru.yandex.practicum.client.WarehouseClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CartService {

    private final CartRepository cartRepository;
    private final CartItemRepository cartItemRepository;
    private final ShoppingCartMapper shoppingCartMapper;
    private final WarehouseClient warehouseClient;

    public ShoppingCartDto getShoppingCart(String username) {
        log.info("[CartService] Getting shopping cart for user: {}", username);
        log.debug("[CartService] Username validation for: {}", username);

        validateUsername(username);

        long startTime = System.currentTimeMillis();
        try {
            // Используем новый метод, который возвращает любую корзину (активную или деактивированную)
            Cart cart = cartRepository.findCartWithItemsByUsername(username)
                    .orElseGet(() -> {
                        log.info("[CartService] Creating new cart for user: {}", username);
                        return createNewCart(username);
                    });

            log.debug("[CartService] Found cart ID: {} with status: {} and {} items",
                    cart.getShoppingCartId(),
                    cart.getStatus(),
                    cart.getItems() != null ? cart.getItems().size() : 0);

            ShoppingCartDto result = shoppingCartMapper.toDto(cart);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[CartService] Cart retrieval completed for user: {} in {} ms", username, duration);

            return result;

        } catch (Exception e) {
            log.error("[CartService] Error getting cart for user: {}", username, e);
            throw e;
        }
    }

    @Transactional
    public ShoppingCartDto addProductsToCart(String username, Map<String, Integer> products) {
        log.info("[CartService] Adding {} products to cart for user: {}",
                products.size(), username);
        log.debug("[CartService] Products details: {}", products);

        validateUsername(username);

        long startTime = System.currentTimeMillis();
        try {
            // Используем метод, который возвращает только активную корзину
            Cart cart = cartRepository.findActiveCartWithItems(username)
                    .orElseGet(() -> {
                        log.info("[CartService] Creating new cart for adding products, user: {}", username);
                        return createNewCart(username);
                    });

            log.debug("[CartService] Found active cart ID: {} with {} items",
                    cart.getShoppingCartId(),
                    cart.getItems() != null ? cart.getItems().size() : 0);

            // Проверяем, что корзина активна
            if (cart.getStatus() != Cart.CartStatus.ACTIVE) {
                log.error("[CartService] Cannot add products to deactivated cart for user: {} with ID: {}",
                        username, cart.getShoppingCartId());
                throw new CartDeactivatedException(username, cart.getShoppingCartId());
            }

            int addedCount = 0;
            int updatedCount = 0;

            // Проверяем доступность товаров на складе
            Map<String, Integer> validProducts = new HashMap<>();
            for (Map.Entry<String, Integer> entry : products.entrySet()) {
                UUID productId;
                try {
                    productId = UUID.fromString(entry.getKey());
                    log.debug("[CartService] Validating product: {}, quantity: {}",
                            productId, entry.getValue());
                } catch (IllegalArgumentException e) {
                    log.warn("[CartService] Invalid UUID format: {}, skipping", entry.getKey());
                    continue;
                }

                Integer targetQuantity = entry.getValue();

                if (targetQuantity <= 0) {
                    log.warn("[CartService] Invalid quantity {} for product {}, skipping",
                            targetQuantity, productId);
                    continue;
                }

                // Добавляем в валидные продукты для проверки на складе
                validProducts.put(entry.getKey(), targetQuantity);
            }

            if (!validProducts.isEmpty()) {
                // Создаем временную корзину для проверки доступности товаров
                ShoppingCartDto tempCart = new ShoppingCartDto();
                tempCart.setShoppingCartId(cart.getShoppingCartId());
                tempCart.setProducts(validProducts);

                try {
                    // Проверяем доступность товаров через Warehouse сервис
                    log.debug("[CartService] Checking product availability with warehouse service");
                    warehouseClient.checkProductQuantityEnoughForShoppingCart(tempCart);
                    log.debug("[CartService] All products are available in warehouse");
                } catch (Exception e) {
                    log.error("[CartService] Warehouse check failed: {}", e.getMessage());
                    throw new InsufficientStockException("Not enough products in warehouse");
                }
            }

            // Добавляем товары в корзину после успешной проверки
            for (Map.Entry<String, Integer> entry : validProducts.entrySet()) {
                UUID productId = UUID.fromString(entry.getKey());
                Integer targetQuantity = entry.getValue();

                Optional<CartItem> existingItem = cartItemRepository.findByCartShoppingCartIdAndProductId(
                        cart.getShoppingCartId(), productId);

                if (existingItem.isPresent()) {
                    CartItem cartItem = existingItem.get();
                    int oldQuantity = cartItem.getQuantity();
                    log.info("[CartService] Updating product {} quantity from {} to {}",
                            productId, oldQuantity, targetQuantity);
                    cartItem.setQuantity(targetQuantity);
                    cartItemRepository.save(cartItem);
                    updatedCount++;
                } else {
                    log.info("[CartService] Adding new product {} with quantity {}",
                            productId, targetQuantity);
                    CartItem newItem = new CartItem();
                    newItem.setCart(cart);
                    newItem.setProductId(productId);
                    newItem.setQuantity(targetQuantity);
                    cartItemRepository.save(newItem);

                    if (cart.getItems() == null) {
                        cart.setItems(new ArrayList<>());
                    }
                    cart.getItems().add(newItem);
                    addedCount++;
                }
            }

            cartRepository.save(cart);
            log.debug("[CartService] Cart saved, ID: {}", cart.getShoppingCartId());

            Cart updatedCart = cartRepository.findActiveCartWithItems(username)
                    .orElseThrow(() -> {
                        log.error("[CartService] Cart not found after update for user: {}", username);
                        return new CartNotFoundException(username, cart.getShoppingCartId());
                    });

            ShoppingCartDto result = shoppingCartMapper.toDto(updatedCart);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[CartService] Products added successfully. Added: {}, Updated: {}, Total time: {} ms",
                    addedCount, updatedCount, duration);
            log.debug("[CartService] Final cart state for user {}: {} items",
                    username, updatedCart.getItems().size());

            return result;

        } catch (Exception e) {
            log.error("[CartService] Error adding products to cart for user: {}", username, e);
            throw e;
        }
    }

    @Transactional
    public void deactivateCart(String username) {
        log.info("[CartService] Deactivating cart for user: {}", username);

        validateUsername(username);

        long startTime = System.currentTimeMillis();
        try {
            // Используем метод поиска активной корзины
            Optional<Cart> cartOptional = cartRepository.findByUsernameAndStatus(username, Cart.CartStatus.ACTIVE);

            if (cartOptional.isPresent()) {
                Cart cart = cartOptional.get();
                log.debug("[CartService] Found active cart ID: {} for deactivation", cart.getShoppingCartId());
                cart.setStatus(Cart.CartStatus.DEACTIVATED);
                cartRepository.save(cart);

                long duration = System.currentTimeMillis() - startTime;
                log.info("[CartService] Cart deactivated successfully for user: {} in {} ms", username, duration);
            } else {
                log.warn("[CartService] No active cart found for user: {}, nothing to deactivate", username);
            }
        } catch (Exception e) {
            log.error("[CartService] Error deactivating cart for user: {}", username, e);
            throw e;
        }
    }

    @Transactional
    public ShoppingCartDto removeProductsFromCart(String username, List<UUID> productIds) {
        log.info("[CartService] Removing {} products from cart for user: {}",
                productIds.size(), username);
        log.debug("[CartService] Product IDs to remove: {}", productIds);

        validateUsername(username);

        long startTime = System.currentTimeMillis();
        try {
            // Используем метод, который возвращает только активную корзину
            Cart cart = cartRepository.findActiveCartWithItems(username)
                    .orElseThrow(() -> {
                        log.error("[CartService] Active cart not found for user: {}", username);
                        return new CartNotFoundException(username, null);
                    });

            // Проверяем, что корзина активна
            if (cart.getStatus() != Cart.CartStatus.ACTIVE) {
                log.error("[CartService] Cannot remove products from deactivated cart for user: {} with ID: {}",
                        username, cart.getShoppingCartId());
                throw new CartDeactivatedException(username, cart.getShoppingCartId());
            }

            if (cart.getItems().isEmpty()) {
                log.warn("[CartService] Cart is empty for user: {}", username);
                throw new NoProductsInShoppingCartException();
            }

            List<UUID> cartProductIds = cart.getItems().stream()
                    .map(CartItem::getProductId)
                    .collect(Collectors.toList());

            boolean allProductsExist = productIds.stream()
                    .allMatch(cartProductIds::contains);

            if (!allProductsExist) {
                log.warn("[CartService] Not all products exist in cart. Requested: {}, Available: {}",
                        productIds, cartProductIds);
                throw new NoProductsInShoppingCartException();
            }

            log.debug("[CartService] Removing products from cart ID: {}", cart.getShoppingCartId());
            cartItemRepository.deleteByCartIdAndProductIds(cart.getShoppingCartId(), productIds);

            Cart updatedCart = cartRepository.findActiveCartWithItems(username)
                    .orElseThrow(() -> {
                        log.error("[CartService] Cart not found after removal for user: {}", username);
                        return new CartNotFoundException(username, cart.getShoppingCartId());
                    });

            ShoppingCartDto result = shoppingCartMapper.toDto(updatedCart);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[CartService] Products removed successfully. Remaining items: {}, Time: {} ms",
                    updatedCart.getItems().size(), duration);

            return result;
        } catch (Exception e) {
            log.error("[CartService] Error removing products from cart for user: {}", username, e);
            throw e;
        }
    }

    @Transactional
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        log.info("[CartService] Changing product quantity for user: {}, product: {}, new quantity: {}",
                username, request.getProductId(), request.getNewQuantity());

        validateUsername(username);

        long startTime = System.currentTimeMillis();
        try {
            // Используем метод, который возвращает только активную корзину
            Cart cart = cartRepository.findActiveCartWithItems(username)
                    .orElseThrow(() -> {
                        log.error("[CartService] Active cart not found for user: {}", username);
                        return new CartNotFoundException(username, null);
                    });

            // Проверяем, что корзина активна
            if (cart.getStatus() != Cart.CartStatus.ACTIVE) {
                log.error("[CartService] Cannot change quantity in deactivated cart for user: {} with ID: {}",
                        username, cart.getShoppingCartId());
                throw new CartDeactivatedException(username, cart.getShoppingCartId());
            }

            CartItem cartItem = cartItemRepository.findByCartShoppingCartIdAndProductId(
                            cart.getShoppingCartId(), request.getProductId())
                    .orElseThrow(() -> {
                        log.warn("[CartService] Product {} not found in cart for user: {}",
                                request.getProductId(), username);
                        return new NoProductsInShoppingCartException();
                    });

            log.debug("[CartService] Current quantity for product {}: {}",
                    request.getProductId(), cartItem.getQuantity());

            if (request.getNewQuantity() <= 0) {
                log.info("[CartService] Removing product {} from cart (quantity <= 0)", request.getProductId());
                cartItemRepository.delete(cartItem);
            } else {
                log.info("[CartService] Updating product {} quantity from {} to {}",
                        request.getProductId(), cartItem.getQuantity(), request.getNewQuantity());
                cartItem.setQuantity(request.getNewQuantity());
                cartItemRepository.save(cartItem);
            }

            Cart updatedCart = cartRepository.findActiveCartWithItems(username)
                    .orElseThrow(() -> {
                        log.error("[CartService] Cart not found after quantity change for user: {}", username);
                        return new CartNotFoundException(username, cart.getShoppingCartId());
                    });

            ShoppingCartDto result = shoppingCartMapper.toDto(updatedCart);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[CartService] Product quantity changed successfully in {} ms", duration);

            return result;
        } catch (Exception e) {
            log.error("[CartService] Error changing product quantity for user: {}", username, e);
            throw e;
        }
    }

    private Cart createNewCart(String username) {
        log.debug("[CartService] Creating new cart entity for user: {}", username);

        Cart newCart = new Cart();
        newCart.setUsername(username);
        newCart.setStatus(Cart.CartStatus.ACTIVE);

        Cart savedCart = cartRepository.save(newCart);
        log.info("[CartService] New cart created with ID: {} for user: {}",
                savedCart.getShoppingCartId(), username);

        return savedCart;
    }

    private void validateUsername(String username) {
        log.debug("[CartService] Validating username: {}", username);

        if (username == null || username.trim().isEmpty()) {
            log.error("[CartService] Username validation failed: username is null or empty");
            throw new NotAuthorizedUserException();
        }

        log.debug("[CartService] Username validation passed");
    }
}