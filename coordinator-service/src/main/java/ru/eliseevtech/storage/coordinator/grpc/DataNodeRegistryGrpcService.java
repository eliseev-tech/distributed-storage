package ru.eliseevtech.storage.coordinator.grpc;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.eliseevtech.storage.coordinator.proto.*;
import ru.eliseevtech.storage.coordinator.registry.DataNodeRegistry;

@GrpcService
@RequiredArgsConstructor
public class DataNodeRegistryGrpcService extends DataNodeRegistryServiceGrpc.DataNodeRegistryServiceImplBase {

    private final DataNodeRegistry registry;

    @Override
    public void registerDataNode(RegisterDataNodeRequest request,
                                 StreamObserver<RegisterDataNodeResponse> responseObserver) {
        String nodeId = registry.register(request.getHost(), request.getPort());
        RegisterDataNodeResponse response = RegisterDataNodeResponse.newBuilder()
                .setNodeId(nodeId)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void unregisterDataNode(UnregisterDataNodeRequest request,
                                   StreamObserver<UnregisterDataNodeResponse> responseObserver) {
        registry.unregister(request.getNodeId());
        responseObserver.onNext(UnregisterDataNodeResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest request,
                          StreamObserver<HeartbeatResponse> responseObserver) {
        boolean known = registry.getActiveNodes().stream()
                .anyMatch(n -> n.getNodeId().equals(request.getNodeId()));
        if (known) {
            registry.heartbeat(request.getNodeId());
        }
        HeartbeatResponse response = HeartbeatResponse.newBuilder()
                .setKnown(known)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
