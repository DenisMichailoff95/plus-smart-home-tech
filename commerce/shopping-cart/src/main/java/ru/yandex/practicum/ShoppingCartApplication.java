package ru.yandex.practicum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients
public class ShoppingCartApplication {
    private static final Logger log = LoggerFactory.getLogger(ShoppingCartApplication.class);

    public static void main(String[] args) {
        log.info("Starting Shopping Cart application...");
        log.debug("Application arguments count: {}", args.length);

        try {
            SpringApplication.run(ShoppingCartApplication.class, args);
            log.info("Shopping Cart application started successfully");
            log.info("Service registered with Eureka as: shopping-cart");
        } catch (Exception e) {
            log.error("Failed to start Shopping Cart application", e);
            throw e;
        }
    }
}