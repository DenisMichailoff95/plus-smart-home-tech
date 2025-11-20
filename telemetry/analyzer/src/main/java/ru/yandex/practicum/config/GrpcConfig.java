package ru.yandex.practicum.config;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

@Configuration
public class GrpcConfig {

    @Bean
    public ManagedChannel hubRouterChannel() {
        return ManagedChannelBuilder
                .forAddress("localhost", 59090)
                .usePlaintext()
                .build();
    }

    @Bean
    public HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterControllerBlockingStub(ManagedChannel hubRouterChannel) {
        return HubRouterControllerGrpc.newBlockingStub(hubRouterChannel);
    }
}