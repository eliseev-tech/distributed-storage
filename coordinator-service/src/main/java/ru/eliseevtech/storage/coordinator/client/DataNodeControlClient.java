package ru.eliseevtech.storage.coordinator.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import ru.eliseevtech.storage.datanode.proto.DataNodeControlServiceGrpc;
import ru.eliseevtech.storage.datanode.proto.DeleteUploadRequest;
import ru.eliseevtech.storage.datanode.proto.GetUploadStatsRequest;
import ru.eliseevtech.storage.datanode.proto.GetUploadStatsResponse;

@Slf4j
public class DataNodeControlClient {

    public void deleteUpload(String host, int port, String uploadId) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            DataNodeControlServiceGrpc.DataNodeControlServiceBlockingStub stub =
                    DataNodeControlServiceGrpc.newBlockingStub(channel);
            stub.deleteUpload(DeleteUploadRequest.newBuilder()
                    .setUploadId(uploadId)
                    .build());
        } finally {
            channel.shutdown();
        }
    }

    public GetUploadStatsResponse getUploadStats(String host, int port, String uploadId) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            DataNodeControlServiceGrpc.DataNodeControlServiceBlockingStub stub =
                    DataNodeControlServiceGrpc.newBlockingStub(channel);
            return stub.getUploadStats(GetUploadStatsRequest.newBuilder()
                    .setUploadId(uploadId)
                    .build());
        } finally {
            channel.shutdown();
        }
    }

}
