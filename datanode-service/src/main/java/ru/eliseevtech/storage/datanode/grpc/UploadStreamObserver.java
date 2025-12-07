package ru.eliseevtech.storage.datanode.grpc;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import ru.eliseevtech.storage.datanode.proto.UploadChunk;
import ru.eliseevtech.storage.datanode.proto.UploadResponse;
import ru.eliseevtech.storage.datanode.service.FileStorageService;

@Slf4j
public class UploadStreamObserver implements StreamObserver<UploadChunk> {

    private final FileStorageService storageService;
    private final StreamObserver<UploadResponse> responseObserver;

    private String uploadId;
    private int lastChunkIndex = -1;
    private long totalBytes = 0;

    public UploadStreamObserver(FileStorageService storageService,
                                StreamObserver<UploadResponse> responseObserver) {
        this.storageService = storageService;
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(UploadChunk chunk) {
        if (uploadId == null) {
            uploadId = chunk.getUploadId();
        }
        storageService.appendChunk(chunk.getUploadId(), chunk.getChunkIndex(), chunk.getData().toByteArray());
        lastChunkIndex = chunk.getChunkIndex();
        totalBytes += chunk.getData().size();
    }

    @Override
    public void onError(Throwable t) {
        log.warn("Upload stream error", t);
    }

    @Override
    public void onCompleted() {
        UploadResponse response = UploadResponse.newBuilder()
                .setUploadId(uploadId == null ? "" : uploadId)
                .setUploadedChunks(lastChunkIndex + 1)
                .setUploadedBytes(totalBytes)
                .setSuccess(true)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
