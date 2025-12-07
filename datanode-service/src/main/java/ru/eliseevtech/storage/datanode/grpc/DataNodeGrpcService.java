package ru.eliseevtech.storage.datanode.grpc;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.eliseevtech.storage.datanode.proto.DataNodeServiceGrpc;
import ru.eliseevtech.storage.datanode.proto.DownloadChunk;
import ru.eliseevtech.storage.datanode.proto.DownloadRequest;
import ru.eliseevtech.storage.datanode.proto.UploadChunk;
import ru.eliseevtech.storage.datanode.proto.UploadResponse;
import ru.eliseevtech.storage.datanode.service.FileStorageService;

@GrpcService
@RequiredArgsConstructor
public class DataNodeGrpcService extends DataNodeServiceGrpc.DataNodeServiceImplBase {

    private final FileStorageService storageService;

    @Override
    public StreamObserver<UploadChunk> uploadFileStream(StreamObserver<UploadResponse> responseObserver) {
        return new UploadStreamObserver(storageService, responseObserver);
    }

    @Override
    public void downloadFileStream(DownloadRequest request,
                                   StreamObserver<DownloadChunk> responseObserver) {
        storageService.streamChunks(request.getUploadId(), 1024 * 1024, (index, data, isLast) -> {
            DownloadChunk chunk = DownloadChunk.newBuilder()
                    .setChunkIndex(index)
                    .setData(com.google.protobuf.ByteString.copyFrom(data))
                    .setIsLast(isLast)
                    .build();
            responseObserver.onNext(chunk);
            if (isLast) {
                responseObserver.onCompleted();
            }
        });
    }

}
