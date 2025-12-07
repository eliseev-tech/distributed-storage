package ru.eliseevtech.storage.coordinator.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.eliseevtech.storage.coordinator.proto.CoordinatorServiceGrpc;
import ru.eliseevtech.storage.coordinator.proto.FinalizeUploadRequest;
import ru.eliseevtech.storage.coordinator.proto.FinalizeUploadResponse;
import ru.eliseevtech.storage.coordinator.proto.GetUploadStatusRequest;
import ru.eliseevtech.storage.coordinator.proto.GetUploadStatusResponse;
import ru.eliseevtech.storage.coordinator.proto.InitiateDownloadRequest;
import ru.eliseevtech.storage.coordinator.proto.InitiateDownloadResponse;
import ru.eliseevtech.storage.coordinator.proto.InitiateUploadRequest;
import ru.eliseevtech.storage.coordinator.proto.InitiateUploadResponse;
import ru.eliseevtech.storage.coordinator.service.DownloadInitResult;
import ru.eliseevtech.storage.coordinator.service.DownloadService;
import ru.eliseevtech.storage.coordinator.service.GetUploadStatusResult;
import ru.eliseevtech.storage.coordinator.service.InitiateUploadResult;
import ru.eliseevtech.storage.coordinator.service.UploadService;

@GrpcService
@RequiredArgsConstructor
public class CoordinatorGrpcService extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {

    private final UploadService uploadService;
    private final DownloadService downloadService;

    @Override
    public void initiateUpload(InitiateUploadRequest request,
                               StreamObserver<InitiateUploadResponse> responseObserver) {
        try {
            InitiateUploadResult result = uploadService.initiateUpload(
                    request.getFilePath(), request.getFileSize(), request.getResume());
            String[] parts = result.getDataNodeAddress().split(":");
            String address = parts[0] + ":" + parts[1];

            InitiateUploadResponse response = InitiateUploadResponse.newBuilder()
                    .setUploadId(result.getUploadId())
                    .setDataNodeAddress(address)
                    .setChunkSize(result.getChunkSize())
                    .setResumed(result.isResumed())
                    .setLastChunkIndex(result.getLastChunkIndex())
                    .setBytesUploaded(result.getBytesUploaded())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage())
                    .withCause(e).asRuntimeException());
        }
    }

    @Override
    public void finalizeUpload(FinalizeUploadRequest request,
                               StreamObserver<FinalizeUploadResponse> responseObserver) {
        try {
            uploadService.finalizeUpload(request.getUploadId(), request.getFilePath());
            FinalizeUploadResponse response = FinalizeUploadResponse.newBuilder()
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            FinalizeUploadResponse response = FinalizeUploadResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void initiateDownload(InitiateDownloadRequest request,
                                 StreamObserver<InitiateDownloadResponse> responseObserver) {
        try {
            DownloadInitResult result = downloadService.initiateDownload(request.getFilePath());
            InitiateDownloadResponse response = InitiateDownloadResponse.newBuilder()
                    .setUploadId(result.getUploadId())
                    .setDataNodeAddress(result.getDataNodeAddress())
                    .setFileSize(result.getFileSize())
                    .setChunkSize(1048576)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage())
                    .withCause(e).asRuntimeException());
        }
    }

    @Override
    public void getUploadStatus(GetUploadStatusRequest request,
                                StreamObserver<GetUploadStatusResponse> responseObserver) {
        try {
            GetUploadStatusResult result = uploadService.getUploadStatus(request.getUploadId());
            GetUploadStatusResponse response = GetUploadStatusResponse.newBuilder()
                    .setUploadId(result.getUploadId())
                    .setStatus(result.getStatus())
                    .setBytesUploaded(result.getBytesUploaded())
                    .setLastChunkIndex(result.getLastChunkIndex())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage())
                    .withCause(e).asRuntimeException());
        }
    }

}
