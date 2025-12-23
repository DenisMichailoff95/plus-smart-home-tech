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
import ru.yandex.practicum.mapper.ShoppingCartMapper;
import ru.yandex.practicum.repository.CartRepository;
import ru.yandex.practicum.repository.CartItemRepository;

import java.util.ArrayList;
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

    public ShoppingCartDto getShoppingCart(String username) {
        validateUsername(username);
        log.info("Getting shopping cart for user: {}", username);

        Cart cart = cartRepository.findActiveCartWithItems(username)
                .orElseGet(() -> createNewCart(username));

        return shoppingCartMapper.toDto(cart);
    }

    @Transactional
    public ShoppingCartDto addProductsToCart(String username, Map<String, Integer> products) {
        validateUsername(username);
        log.info("Adding products to cart for user: {}, products: {}", username, products);

        Cart cart = cartRepository.findActiveCartWithItems(username)
                .orElseGet(() -> createNewCart(username));

        log.info("Found/created cart: {}, items count: {}", cart.getShoppingCartId(),
                cart.getItems() != null ? cart.getItems().size() : "null");

        for (Map.Entry<String, Integer> entry : products.entrySet()) {
            UUID productId;
            try {
                productId = UUID.fromString(entry.getKey());
            } catch (IllegalArgumentException e) {
                log.error("Invalid UUID format: {}", entry.getKey());
                continue;
            }

            Integer targetQuantity = entry.getValue();

            Optional<CartItem> existingItem = cartItemRepository.findByCartShoppingCartIdAndProductId(cart.getShoppingCartId(), productId);

            if (existingItem.isPresent()) {
                CartItem cartItem = existingItem.get();
                log.info("Updating existing item: {} from {} to {}",
                        productId, cartItem.getQuantity(), targetQuantity);
                cartItem.setQuantity(targetQuantity);
                cartItemRepository.save(cartItem);
            } else {
                log.info("Creating new item: {} with quantity {}", productId, targetQuantity);
                CartItem newItem = new CartItem();
                newItem.setCart(cart);
                newItem.setProductId(productId);
                newItem.setQuantity(targetQuantity);
                cartItemRepository.save(newItem);

                if (cart.getItems() == null) {
                    cart.setItems(new ArrayList<>());
                }
                cart.getItems().add(newItem);
            }
        }

        cartRepository.save(cart);

        Cart updatedCart = cartRepository.findActiveCartWithItems(username)
                .orElseThrow(() -> new CartNotFoundException(username, cart.getShoppingCartId()));

        log.info("Final cart before mapping - ID: {}, items count: {}",
                updatedCart.getShoppingCartId(),
                updatedCart.getItems() != null ? updatedCart.getItems().size() : "null");

        return shoppingCartMapper.toDto(updatedCart);
    }

    @Transactional
    public void deactivateCart(String username) {
        validateUsername(username);
        log.info("Deactivating cart for user: {}", username);

        cartRepository.findByUsernameAndStatus(username, Cart.CartStatus.ACTIVE)
                .ifPresent(cart -> {
                    cart.setStatus(Cart.CartStatus.DEACTIVATED);
                    cartRepository.save(cart);
                    log.info("Cart deactivated for user: {}", username);
                });

        log.info("No active cart found for user: {}, nothing to deactivate", username);
    }

    @Transactional
    public ShoppingCartDto removeProductsFromCart(String username, List<UUID> productIds) {
        validateUsername(username);
        log.info("Removing products from cart for user: {}, productIds: {}", username, productIds);

        Cart cart = cartRepository.findActiveCartWithItems(username)
                .orElseThrow(() -> new CartNotFoundException(username, null));

        if (cart.getItems().isEmpty()) {
            throw new NoProductsInShoppingCartException();
        }

        List<UUID> cartProductIds = cart.getItems().stream()
                .map(CartItem::getProductId)
                .collect(Collectors.toList());

        boolean allProductsExist = productIds.stream()
                .allMatch(cartProductIds::contains);

        if (!allProductsExist) {
            throw new NoProductsInShoppingCartException();
        }

        cartItemRepository.deleteByCartIdAndProductIds(cart.getShoppingCartId(), productIds);

        Cart updatedCart = cartRepository.findActiveCartWithItems(username)
                .orElseThrow(() -> new CartNotFoundException(username, cart.getShoppingCartId()));

        return shoppingCartMapper.toDto(updatedCart);
    }

    @Transactional
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        validateUsername(username);
        log.info("Changing product quantity for user: {}, request: {}", username, request);

        Cart cart = cartRepository.findActiveCartWithItems(username)
                .orElseThrow(() -> new CartNotFoundException(username, null));

        CartItem cartItem = cartItemRepository.findByCartShoppingCartIdAndProductId(
                        cart.getShoppingCartId(), request.getProductId())
                .orElseThrow(() -> new NoProductsInShoppingCartException());

        if (request.getNewQuantity() <= 0) {
            cartItemRepository.delete(cartItem);
        } else {
            cartItem.setQuantity(request.getNewQuantity());
            cartItemRepository.save(cartItem);
        }

        Cart updatedCart = cartRepository.findActiveCartWithItems(username)
                .orElseThrow(() -> new CartNotFoundException(username, cart.getShoppingCartId()));

        return shoppingCartMapper.toDto(updatedCart);
    }

    private Cart createNewCart(String username) {
        Cart newCart = new Cart();
        newCart.setUsername(username);
        newCart.setStatus(Cart.CartStatus.ACTIVE);
        return cartRepository.save(newCart);
    }

    private void validateUsername(String username) {
        if (username == null || username.trim().isEmpty()) {
            throw new NotAuthorizedUserException();
        }
    }
}