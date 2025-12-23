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
public class WarehouseApplication {
    private static final Logger log = LoggerFactory.getLogger(WarehouseApplication.class);

    public static void main(String[] args) {
        log.info("Starting Warehouse application...");
        log.debug("Startup configuration loaded");

        try {
            SpringApplication.run(WarehouseApplication.class, args);
            log.info("Warehouse application started successfully");
            log.info("Inventory management service is now operational");
        } catch (Exception e) {
            log.error("Failed to start Warehouse application", e);
            throw e;
        }
    }
}