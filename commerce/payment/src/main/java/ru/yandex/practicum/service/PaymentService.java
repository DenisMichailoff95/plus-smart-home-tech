package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.client.OrderPaymentClient;
import ru.yandex.practicum.client.ShoppingStorePaymentClient;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.dto.payment.PaymentStatus;
import ru.yandex.practicum.dto.shoppingstore.ProductDto;
import ru.yandex.practicum.entity.Payment;
import ru.yandex.practicum.exception.NoOrderFoundException;
import ru.yandex.practicum.exception.NotEnoughInfoInOrderToCalculateException;
import ru.yandex.practicum.mapper.PaymentMapper;
import ru.yandex.practicum.repository.PaymentRepository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentMapper paymentMapper;
    private final OrderPaymentClient orderPaymentClient;
    private final ShoppingStorePaymentClient shoppingStorePaymentClient;

    @Transactional
    public PaymentDto payment(OrderDto orderDto) {
        log.info("Processing payment for order: {}", orderDto.getOrderId());

        validateOrderForCalculation(orderDto);

        BigDecimal productCost = calculateProductCostInternal(orderDto);
        BigDecimal deliveryCost = orderDto.getDeliveryPrice();

        if (deliveryCost == null) {
            log.error("Delivery cost is null for order: {}", orderDto.getOrderId());
            throw new NotEnoughInfoInOrderToCalculateException("Delivery cost is not specified");
        }

        BigDecimal totalCost = calculateTotalCost(productCost, deliveryCost);
        BigDecimal feeTotal = calculateFee(productCost);

        Payment payment = new Payment();
        payment.setOrderId(orderDto.getOrderId());
        payment.setStatus(PaymentStatus.PENDING);
        payment.setTotalPayment(totalCost);
        payment.setDeliveryTotal(deliveryCost);
        payment.setFeeTotal(feeTotal);

        Payment savedPayment = paymentRepository.save(payment);
        log.info("Payment created with ID: {} for order: {}, total: ${} (products: ${}, delivery: ${}, fee: ${})",
                savedPayment.getPaymentId(),
                orderDto.getOrderId(),
                totalCost, productCost, deliveryCost, feeTotal);

        return paymentMapper.toDto(savedPayment);
    }

    public BigDecimal getTotalCost(OrderDto orderDto) {
        log.info("Calculating total cost for order: {}", orderDto.getOrderId());

        validateOrderForCalculation(orderDto);

        BigDecimal productCost = calculateProductCostInternal(orderDto);
        BigDecimal deliveryCost = orderDto.getDeliveryPrice();

        if (deliveryCost == null) {
            log.error("Delivery cost is null for order: {}", orderDto.getOrderId());
            throw new NotEnoughInfoInOrderToCalculateException("Delivery cost is not specified");
        }

        BigDecimal totalCost = calculateTotalCost(productCost, deliveryCost);
        log.info("Total cost for order {}: ${} (products: ${}, delivery: ${})",
                orderDto.getOrderId(), totalCost, productCost, deliveryCost);

        return totalCost;
    }

    public BigDecimal productCost(OrderDto orderDto) {
        log.info("Calculating product cost for order: {}", orderDto.getOrderId());

        validateOrderForCalculation(orderDto);

        BigDecimal cost = calculateProductCostInternal(orderDto);
        log.info("Product cost for order {}: ${}", orderDto.getOrderId(), cost);

        return cost;
    }

    @Transactional
    public void paymentFailed(UUID paymentId) {
        log.info("Processing failed payment for payment ID: {}", paymentId);

        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> {
                    log.error("Payment not found: {}", paymentId);
                    return new NoOrderFoundException(paymentId);
                });

        payment.setStatus(PaymentStatus.FAILED);
        paymentRepository.save(payment);

        try {
            orderPaymentClient.paymentFailed(payment.getOrderId());
            log.info("Order {} status updated to PAYMENT_FAILED", payment.getOrderId());
        } catch (Exception e) {
            log.error("Failed to update order status for payment {}: {}", paymentId, e.getMessage(), e);
            // Payment is already marked as FAILED, but we should log the integration failure
        }

        log.info("Payment {} marked as FAILED for order: {}", paymentId, payment.getOrderId());
    }

    private BigDecimal calculateProductCostInternal(OrderDto orderDto) {
        return calculateRealProductCost(orderDto.getProducts());
    }

    private BigDecimal calculateRealProductCost(Map<String, Integer> products) {
        if (products == null || products.isEmpty()) {
            log.warn("Product list is empty");
            return BigDecimal.ZERO;
        }

        BigDecimal totalCost = BigDecimal.ZERO;
        int processedProducts = 0;
        int failedProducts = 0;

        for (Map.Entry<String, Integer> entry : products.entrySet()) {
            try {
                UUID productId = UUID.fromString(entry.getKey());
                Integer quantity = entry.getValue();

                if (quantity <= 0) {
                    log.warn("Invalid quantity {} for product {}, skipping", quantity, productId);
                    failedProducts++;
                    continue;
                }

                ProductDto productDto = shoppingStorePaymentClient.getProduct(productId);

                if (productDto != null && productDto.getPrice() != null) {
                    BigDecimal productPrice = productDto.getPrice();
                    BigDecimal itemTotal = productPrice.multiply(new BigDecimal(quantity));
                    totalCost = totalCost.add(itemTotal);
                    processedProducts++;
                    log.debug("Product {}: {} x ${} = ${}",
                            productId, quantity, productPrice, itemTotal);
                } else {
                    log.warn("Product {} not found or has no price, using default price", productId);
                    BigDecimal defaultPrice = new BigDecimal("100.00");
                    BigDecimal itemTotal = defaultPrice.multiply(new BigDecimal(quantity));
                    totalCost = totalCost.add(itemTotal);
                    processedProducts++;
                }
            } catch (IllegalArgumentException e) {
                log.error("Invalid UUID format: {}, skipping", entry.getKey());
                failedProducts++;
                continue;
            } catch (Exception e) {
                log.error("Error getting product price for {}: {}", entry.getKey(), e.getMessage());
                BigDecimal defaultPrice = new BigDecimal("100.00");
                BigDecimal itemTotal = defaultPrice.multiply(new BigDecimal(entry.getValue()));
                totalCost = totalCost.add(itemTotal);
                processedProducts++;
            }
        }

        log.info("Product cost calculation: {} products processed, {} failed, total: ${}",
                processedProducts, failedProducts, totalCost);

        return totalCost.setScale(2, RoundingMode.HALF_UP);
    }

    private BigDecimal calculateTotalCost(BigDecimal productCost, BigDecimal deliveryCost) {
        log.debug("Calculating total cost: product=${}, delivery=${}", productCost, deliveryCost);

        BigDecimal fee = calculateFee(productCost);
        BigDecimal total = productCost.add(deliveryCost).add(fee);

        log.debug("Total cost breakdown: product=${} + delivery=${} + fee=${} = total=${}",
                productCost, deliveryCost, fee, total);

        return total.setScale(2, RoundingMode.HALF_UP);
    }

    private BigDecimal calculateFee(BigDecimal productCost) {
        BigDecimal fee = productCost.multiply(new BigDecimal("0.10"))
                .setScale(2, RoundingMode.HALF_UP);
        log.debug("Calculated fee (10% of ${}): ${}", productCost, fee);
        return fee;
    }

    private void validateOrderForCalculation(OrderDto orderDto) {
        log.debug("Validating order for calculation: {}", orderDto.getOrderId());

        if (orderDto == null) {
            log.error("Order cannot be null");
            throw new IllegalArgumentException("Order cannot be null");
        }

        if (orderDto.getOrderId() == null) {
            log.error("Order ID is required");
            throw new NotEnoughInfoInOrderToCalculateException("Order ID is required");
        }

        if (orderDto.getProducts() == null || orderDto.getProducts().isEmpty()) {
            log.error("Order products cannot be empty");
            throw new NotEnoughInfoInOrderToCalculateException("Order products cannot be empty");
        }

        log.debug("Order validation passed for: {}", orderDto.getOrderId());
    }

    @Transactional
    public void paymentRefund(UUID paymentId) {
        log.info("Processing payment refund for payment ID: {}", paymentId);

        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> {
                    log.error("Payment not found for refund: {}", paymentId);
                    return new NoOrderFoundException(paymentId);
                });

        // Validate payment can be refunded
        if (payment.getStatus() != PaymentStatus.SUCCESS) {
            log.error("Payment {} cannot be refunded, current status: {}",
                    paymentId, payment.getStatus());
            throw new IllegalStateException("Only SUCCESS payments can be refunded");
        }

        payment.setStatus(PaymentStatus.SUCCESS); // Keep as SUCCESS for refund tracking
        paymentRepository.save(payment);

        try {
            orderPaymentClient.paymentSuccess(payment.getOrderId());
            log.info("Order {} status updated for refund", payment.getOrderId());
        } catch (Exception e) {
            log.error("Failed to update order status for refund {}: {}", paymentId, e.getMessage(), e);
            throw new RuntimeException("Failed to process refund", e);
        }

        log.info("Payment {} refund processed as SUCCESS for order: {}",
                paymentId, payment.getOrderId());
    }
}