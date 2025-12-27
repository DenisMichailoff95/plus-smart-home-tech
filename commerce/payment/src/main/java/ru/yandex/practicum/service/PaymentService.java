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
        log.info("Payment created with ID: {} for order: {}",
                savedPayment.getPaymentId(), orderDto.getOrderId());

        return paymentMapper.toDto(savedPayment);
    }

    public BigDecimal getTotalCost(OrderDto orderDto) {
        log.info("Calculating total cost for order: {}", orderDto.getOrderId());

        validateOrderForCalculation(orderDto);

        BigDecimal productCost = calculateProductCostInternal(orderDto);
        BigDecimal deliveryCost = orderDto.getDeliveryPrice();

        if (deliveryCost == null) {
            throw new NotEnoughInfoInOrderToCalculateException("Delivery cost is not specified");
        }

        return calculateTotalCost(productCost, deliveryCost);
    }

    public BigDecimal productCost(OrderDto orderDto) {
        log.info("Calculating product cost for order: {}", orderDto.getOrderId());

        validateOrderForCalculation(orderDto);

        return calculateProductCostInternal(orderDto);
    }

    @Transactional
    public void paymentFailed(UUID paymentId) {
        log.info("Processing failed payment for payment ID: {}", paymentId);

        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoOrderFoundException(paymentId));

        payment.setStatus(PaymentStatus.FAILED);
        paymentRepository.save(payment);

        orderPaymentClient.paymentFailed(payment.getOrderId());

        log.info("Payment {} marked as FAILED", paymentId);
    }

    private BigDecimal calculateProductCostInternal(OrderDto orderDto) {
        return calculateRealProductCost(orderDto.getProducts());
    }

    private BigDecimal calculateRealProductCost(Map<String, Integer> products) {
        if (products == null || products.isEmpty()) {
            return BigDecimal.ZERO;
        }

        BigDecimal totalCost = BigDecimal.ZERO;

        for (Map.Entry<String, Integer> entry : products.entrySet()) {
            try {
                UUID productId = UUID.fromString(entry.getKey());
                Integer quantity = entry.getValue();

                ProductDto productDto = shoppingStorePaymentClient.getProduct(productId);

                if (productDto != null && productDto.getPrice() != null) {
                    BigDecimal productPrice = productDto.getPrice();
                    BigDecimal itemTotal = productPrice.multiply(new BigDecimal(quantity));
                    totalCost = totalCost.add(itemTotal);
                    log.debug("Product {}: {} x {} = {}", productId, productPrice, quantity, itemTotal);
                } else {
                    log.warn("Product {} not found or has no price", productId);
                    BigDecimal defaultPrice = new BigDecimal("100.00");
                    totalCost = totalCost.add(defaultPrice.multiply(new BigDecimal(quantity)));
                }
            } catch (IllegalArgumentException e) {
                log.error("Invalid UUID format: {}", entry.getKey());
                continue;
            } catch (Exception e) {
                log.error("Error getting product price for: {}", entry.getKey(), e);
                BigDecimal defaultPrice = new BigDecimal("100.00");
                totalCost = totalCost.add(defaultPrice.multiply(new BigDecimal(entry.getValue())));
            }
        }

        return totalCost.setScale(2, RoundingMode.HALF_UP);
    }

    private BigDecimal calculateTotalCost(BigDecimal productCost, BigDecimal deliveryCost) {
        BigDecimal fee = calculateFee(productCost);
        return productCost.add(deliveryCost).add(fee);
    }

    private BigDecimal calculateFee(BigDecimal productCost) {
        return productCost.multiply(new BigDecimal("0.10"))
                .setScale(2, RoundingMode.HALF_UP);
    }

    private void validateOrderForCalculation(OrderDto orderDto) {
        if (orderDto == null) {
            throw new IllegalArgumentException("Order cannot be null");
        }

        if (orderDto.getOrderId() == null) {
            throw new NotEnoughInfoInOrderToCalculateException("Order ID is required");
        }

        if (orderDto.getProducts() == null || orderDto.getProducts().isEmpty()) {
            throw new NotEnoughInfoInOrderToCalculateException("Order products cannot be empty");
        }
    }

    @Transactional
    public void paymentRefund(UUID paymentId) {
        log.info("Processing payment refund for payment ID: {}", paymentId);

        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoOrderFoundException(paymentId));

        payment.setStatus(PaymentStatus.SUCCESS);
        paymentRepository.save(payment);

        orderPaymentClient.paymentSuccess(payment.getOrderId());

        log.info("Payment {} refund processed as SUCCESS", paymentId);
    }
}