package ru.yandex.practicum.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties("aggregator.kafka")
public class KafkaConfig {
    private String bootstrapServers;
    private String sensorsTopic;
    private String snapshotsTopic;
    private String consumerGroup;
}