package ru.yandex.practicum.config;

import org.springframework.context.annotation.Configuration;

@Configuration
public class GrpcConfig {
    // Конфигурация gRPC теперь полностью через application.yaml
    // Ручное создание ManagedChannel удалено для избежания дублирования
}