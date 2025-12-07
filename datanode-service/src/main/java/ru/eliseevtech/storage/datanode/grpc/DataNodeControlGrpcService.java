package ru.eliseevtech.storage.datanode.grpc;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.eliseevtech.storage.datanode.model.UploadStats;
import ru.eliseevtech.storage.datanode.proto.DataNodeControlServiceGrpc;
import ru.eliseevtech.storage.datanode.proto.DeleteUploadRequest;
import ru.eliseevtech.storage.datanode.proto.DeleteUploadResponse;
import ru.eliseevtech.storage.datanode.proto.GetUploadStatsRequest;
import ru.eliseevtech.storage.datanode.proto.GetUploadStatsResponse;
import ru.eliseevtech.storage.datanode.service.FileStorageService;

@GrpcService
@RequiredArgsConstructor
public class DataNodeControlGrpcService
        extends DataNodeControlServiceGrpc.DataNodeControlServiceImplBase {

    private final FileStorageService storageService;

    @Override
    public void deleteUpload(DeleteUploadRequest request,
                             StreamObserver<DeleteUploadResponse> responseObserver) {
        storageService.deleteUpload(request.getUploadId());
        DeleteUploadResponse response = DeleteUploadResponse.newBuilder()
                .setSuccess(true)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getUploadStats(GetUploadStatsRequest request,
                               StreamObserver<GetUploadStatsResponse> responseObserver) {
        UploadStats stats = storageService.getUploadStats(request.getUploadId());
        GetUploadStatsResponse response = GetUploadStatsResponse.newBuilder()
                .setUploadId(stats.getUploadId())
                .setChunksCount(stats.getChunksCount())
                .setBytesWritten(stats.getBytesWritten())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
