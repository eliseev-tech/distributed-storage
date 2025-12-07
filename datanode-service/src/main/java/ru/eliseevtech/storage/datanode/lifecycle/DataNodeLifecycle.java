package ru.eliseevtech.storage.datanode.lifecycle;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.eliseevtech.storage.coordinator.proto.DataNodeRegistryServiceGrpc;
import ru.eliseevtech.storage.coordinator.proto.RegisterDataNodeRequest;
import ru.eliseevtech.storage.coordinator.proto.RegisterDataNodeResponse;
import ru.eliseevtech.storage.coordinator.proto.UnregisterDataNodeRequest;
import ru.eliseevtech.storage.datanode.service.DatanodeProperties;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataNodeLifecycle {

    private final DatanodeProperties properties;
    private final CoordinatorClientProperties coordinatorClientProperties;

    private String nodeId;
    private ManagedChannel channel;
    private DataNodeRegistryServiceGrpc.DataNodeRegistryServiceBlockingStub stub;

    @PostConstruct
    public void register() {
        channel = ManagedChannelBuilder
                .forAddress(coordinatorClientProperties.getHost(), coordinatorClientProperties.getPort())
                .usePlaintext()
                .build();
        stub = DataNodeRegistryServiceGrpc.newBlockingStub(channel);
        RegisterDataNodeResponse response = stub.registerDataNode(
                RegisterDataNodeRequest.newBuilder()
                        .setHost(coordinatorClientProperties.getAdvertisedHost())
                        .setPort(properties.getGrpc().getPort())
                        .build());
        nodeId = response.getNodeId();
        log.info("Registered datanode with id {}", nodeId);
    }

    @PreDestroy
    public void unregister() {
        if (stub != null && nodeId != null) {
            stub.unregisterDataNode(UnregisterDataNodeRequest.newBuilder()
                    .setNodeId(nodeId)
                    .build());
        }
        if (channel != null) {
            channel.shutdown();
        }
    }

}
