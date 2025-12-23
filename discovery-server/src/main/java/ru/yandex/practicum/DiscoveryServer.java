package ru.yandex.practicum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

@SpringBootApplication
@EnableEurekaServer
public class DiscoveryServer {
    private static final Logger log = LoggerFactory.getLogger(DiscoveryServer.class);

    public static void main(String[] args) {
        log.info("Starting Discovery Server application...");
        log.debug("Application arguments: {}", String.join(", ", args));

        try {
            SpringApplication.run(DiscoveryServer.class, args);
            log.info("Discovery Server application started successfully");
            log.info("Eureka dashboard available at: http://localhost:8761");
        } catch (Exception e) {
            log.error("Failed to start Discovery Server application", e);
            throw e;
        }
    }
}