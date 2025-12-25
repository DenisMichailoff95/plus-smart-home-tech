package ru.yandex.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.client.ShoppingStoreClient;
import ru.yandex.practicum.dto.common.PageResponse;
import ru.yandex.practicum.dto.shoppingstore.ProductDto;
import ru.yandex.practicum.dto.shoppingstore.SetProductQuantityStateRequest;
import ru.yandex.practicum.service.ProductService;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-store")
public class ShoppingStoreController implements ShoppingStoreClient {

    private final ProductService productService;

    @Override
    @GetMapping
    public PageResponse<ProductDto> getProductsByCategory(
            @RequestParam("category") String category,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "20") int size,
            @RequestParam(value = "sort", required = false) List<String> sort) {

        Pageable pageable = createPageable(page, size, sort);
        Page<ProductDto> productsPage = productService.getProductsByCategory(category, pageable);

        List<PageResponse.SortInfo> sortInfo = convertSort(pageable.getSort());

        return new PageResponse<>(
                productsPage.getContent(),
                productsPage.getNumber(),
                productsPage.getSize(),
                productsPage.getTotalElements(),
                productsPage.getTotalPages(),
                productsPage.isLast(),
                sortInfo
        );
    }

    @Override
    @GetMapping("/{productId}")
    public ProductDto getProduct(@PathVariable("productId") UUID productId) {
        return productService.getProduct(productId);
    }

    @Override
    @PutMapping
    public ProductDto createProduct(@RequestBody ProductDto productDto) {
        return productService.createProduct(productDto);
    }

    @Override
    @PostMapping
    public ProductDto updateProduct(@RequestBody ProductDto productDto) {
        return productService.updateProduct(productDto);
    }

    @Override
    @PostMapping("/removeProductFromStore")
    public Boolean removeProductFromStore(@RequestBody UUID productId) {
        return productService.removeProductFromStore(productId);
    }

    @Override
    @PostMapping("/quantityState")
    public Boolean setQuantityState(@RequestBody SetProductQuantityStateRequest request) {
        return productService.setQuantityState(request);
    }

    @PostMapping(value = "/quantityState", params = {"productId", "quantityState"})
    public Boolean setQuantityStateFromParams(
            @RequestParam("productId") UUID productId,
            @RequestParam("quantityState") String quantityState) {

        SetProductQuantityStateRequest request = new SetProductQuantityStateRequest();
        request.setProductId(productId);
        request.setQuantityState(ru.yandex.practicum.dto.shoppingstore.QuantityState.valueOf(quantityState));

        return productService.setQuantityState(request);
    }

    private Pageable createPageable(int page, int size, List<String> sort) {
        log.info("Creating pageable - page: {}, size: {}, sort: {}", page, size, sort);

        if (sort == null || sort.isEmpty()) {
            return PageRequest.of(page, size);
        }

        try {
            log.info("Sort list size: {}", sort.size());

            if (sort.size() >= 2) {
                String field = sort.get(0).trim();
                String directionStr = sort.get(1).trim();

                log.info("Parsed - field: '{}', direction: '{}'", field, directionStr);

                Sort.Direction direction = Sort.Direction.fromString(directionStr);
                Sort sortObj = Sort.by(direction, field);

                return PageRequest.of(page, size, sortObj);
            } else {
                log.warn("Not enough sort parameters, using default");
                return PageRequest.of(page, size);
            }

        } catch (Exception e) {
            log.error("Error creating pageable: {}", e.getMessage(), e);
            return PageRequest.of(page, size);
        }
    }

    private List<PageResponse.SortInfo> convertSort(Sort sort) {
        return sort.stream()
                .map(order -> new PageResponse.SortInfo(order.getProperty(), order.getDirection().name()))
                .collect(Collectors.toList());
    }
}