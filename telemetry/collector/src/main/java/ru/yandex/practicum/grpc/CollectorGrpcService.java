package ru.yandex.practicum.grpc;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.service.KafkaProducerService;
import ru.yandex.practicum.service.SensorEventHandler;
import com.google.protobuf.Empty;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@GrpcService
public class CollectorGrpcService extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final Map<SensorEventProto.PayloadCase, SensorEventHandler> sensorEventHandlers;
    private final GrpcToModelMapper grpcToModelMapper;
    private final KafkaProducerService kafkaProducerService;

    public CollectorGrpcService(Set<SensorEventHandler> sensorEventHandlers,
                                GrpcToModelMapper grpcToModelMapper,
                                KafkaProducerService kafkaProducerService) {
        this.grpcToModelMapper = grpcToModelMapper;
        this.kafkaProducerService = kafkaProducerService;

        this.sensorEventHandlers = sensorEventHandlers.stream()
                .collect(Collectors.toMap(
                        SensorEventHandler::getMessageType,
                        Function.identity()
                ));
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Received sensor event via gRPC: {}", request.getId());

            if (sensorEventHandlers.containsKey(request.getPayloadCase())) {
                sensorEventHandlers.get(request.getPayloadCase()).handle(request);
            } else {
                throw new IllegalArgumentException("Не могу найти обработчик для события " + request.getPayloadCase());
            }

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to process sensor event via gRPC: {}", request, e);
            responseObserver.onError(new StatusRuntimeException(io.grpc.Status.fromThrowable(e)));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Received hub event via gRPC: {}", request.getHubId());

            var hubEvent = grpcToModelMapper.toHubEvent(request);
            kafkaProducerService.sendHubEvent(hubEvent);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to process hub event via gRPC: {}", request, e);
            responseObserver.onError(new StatusRuntimeException(io.grpc.Status.fromThrowable(e)));
        }
    }
}