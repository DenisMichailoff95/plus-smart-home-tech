package ru.yandex.practicum.grpcclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GrpcClientApplication {
    public static void main(String[] args) {
        SpringApplication.run(GrpcClientApplication.class, args);
    }
}
