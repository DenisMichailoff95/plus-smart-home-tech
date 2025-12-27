package ru.yandex.practicum.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;

import java.math.BigDecimal;

@FeignClient(name = "payment", contextId = "orderPaymentClient", url = "${feign.client.payment.url}")
public interface PaymentServiceClient {

    @PostMapping("/api/v1/payment/productCost")
    BigDecimal productCost(@RequestBody OrderDto orderDto);

    @PostMapping("/api/v1/payment/totalCost")
    BigDecimal getTotalCost(@RequestBody OrderDto orderDto);

    @PostMapping("/api/v1/payment")
    PaymentDto payment(@RequestBody OrderDto orderDto);
}