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
public class ShoppingStoreApplication {
    private static final Logger log = LoggerFactory.getLogger(ShoppingStoreApplication.class);

    public static void main(String[] args) {
        log.info("Starting Shopping Store application...");
        log.debug("Application startup parameters: {}", String.join(", ", args));

        try {
            SpringApplication.run(ShoppingStoreApplication.class, args);
            log.info("Shopping Store application started successfully");
            log.info("Product catalog service is now available");
        } catch (Exception e) {
            log.error("Failed to start Shopping Store application", e);
            throw e;
        }
    }
}