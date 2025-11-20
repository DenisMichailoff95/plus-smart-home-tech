package ru.yandex.practicum.grpcclient.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import net.devh.boot.grpc.client.inject.GrpcClient;
import ru.yandex.practicum.grpc.echo.EchoRequest;
import ru.yandex.practicum.grpc.echo.EchoResponse;
import ru.yandex.practicum.grpc.echo.EchoServiceGrpc;

@Service
public class EchoSender {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @GrpcClient("echo")
    EchoServiceGrpc.EchoServiceBlockingStub echoService;

    @Scheduled(initialDelay = 1000, fixedDelay = 1000)
    public void echo() {
        log.info("echo send");
        EchoResponse echoResponse = echoService.echo(EchoRequest.newBuilder().build());
        log.info("echo response received");
    }
}
