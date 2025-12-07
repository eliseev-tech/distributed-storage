package ru.eliseevtech.storage.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.eliseevtech.storage.coordinator.proto.CoordinatorServiceGrpc;
import ru.eliseevtech.storage.coordinator.proto.FinalizeUploadRequest;
import ru.eliseevtech.storage.coordinator.proto.FinalizeUploadResponse;
import ru.eliseevtech.storage.coordinator.proto.InitiateDownloadRequest;
import ru.eliseevtech.storage.coordinator.proto.InitiateDownloadResponse;
import ru.eliseevtech.storage.coordinator.proto.InitiateUploadRequest;
import ru.eliseevtech.storage.coordinator.proto.InitiateUploadResponse;
import ru.eliseevtech.storage.datanode.proto.DataNodeServiceGrpc;
import ru.eliseevtech.storage.datanode.proto.DownloadRequest;
import ru.eliseevtech.storage.datanode.proto.UploadChunk;
import ru.eliseevtech.storage.datanode.proto.UploadResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@Slf4j
@Component
@RequiredArgsConstructor
public class StorageClient {

    private final CoordinatorClientProperties properties;

    public void upload(String remotePath, String localPath, boolean resume) throws IOException {
        long fileSize = Files.size(Path.of(localPath));

        ManagedChannel coordChannel = ManagedChannelBuilder
                .forAddress(properties.getHost(), properties.getPort())
                .usePlaintext()
                .build();
        CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordStub =
                CoordinatorServiceGrpc.newBlockingStub(coordChannel);

        InitiateUploadResponse init = coordStub.initiateUpload(
                InitiateUploadRequest.newBuilder()
                        .setFilePath(remotePath)
                        .setFileSize(fileSize)
                        .setResume(resume)
                        .build());

        String uploadId = init.getUploadId();
        String[] addrParts = init.getDataNodeAddress().split(":");
        String host = addrParts[0];
        int port = Integer.parseInt(addrParts[1]);
        int chunkSize = init.getChunkSize();

        long bytesAlreadyUploaded = init.getBytesUploaded();
        int lastChunkIndex = init.getLastChunkIndex();

        long remaining = fileSize - bytesAlreadyUploaded;
        long position = bytesAlreadyUploaded;
        int nextChunkIndex = lastChunkIndex + 1;

        ManagedChannel dataNodeChannel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
        DataNodeServiceGrpc.DataNodeServiceStub dataNodeStub =
                DataNodeServiceGrpc.newStub(dataNodeChannel);

        ProgressBar progressBar = new ProgressBar(fileSize);

        StreamObserver<UploadChunk> requestObserver = dataNodeStub.uploadFileStream(
                new StreamObserver<>() {
                    @Override
                    public void onNext(UploadResponse value) {
                        log.info("Upload completed: chunks={}, bytes={}",
                                value.getUploadedChunks(), value.getUploadedBytes());
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Upload error", t);
                    }

                    @Override
                    public void onCompleted() {
                        log.info("Upload stream completed");
                    }
                });

        try (FileChannel fileChannel = FileChannel.open(Path.of(localPath), StandardOpenOption.READ)) {
            fileChannel.position(position);
            ByteBuffer buffer = ByteBuffer.allocate(chunkSize);

            while (remaining > 0) {
                buffer.clear();
                int toRead = (int) Math.min(chunkSize, remaining);
                buffer.limit(toRead);
                int read = fileChannel.read(buffer);
                if (read <= 0) {
                    break;
                }
                buffer.flip();
                byte[] data = new byte[read];
                buffer.get(data);

                UploadChunk chunk = UploadChunk.newBuilder()
                        .setUploadId(uploadId)
                        .setChunkIndex(nextChunkIndex)
                        .setData(ByteString.copyFrom(data))
                        .build();
                requestObserver.onNext(chunk);

                remaining -= read;
                position += read;
                nextChunkIndex++;
                progressBar.update(position);
            }
        }

        requestObserver.onCompleted();
        dataNodeChannel.shutdown();

        FinalizeUploadResponse finalize = coordStub.finalizeUpload(
                FinalizeUploadRequest.newBuilder()
                        .setUploadId(uploadId)
                        .setFilePath(remotePath)
                        .build());
        if (!finalize.getSuccess()) {
            log.error("Finalize failed: {}", finalize.getErrorMessage());
        } else {
            log.info("Finalize succeeded");
        }

        coordChannel.shutdown();
    }

    public void download(String remotePath, String localPath) throws IOException {
        ManagedChannel coordChannel = ManagedChannelBuilder
                .forAddress(properties.getHost(), properties.getPort())
                .usePlaintext()
                .build();
        CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordStub =
                CoordinatorServiceGrpc.newBlockingStub(coordChannel);

        InitiateDownloadResponse init = coordStub.initiateDownload(
                InitiateDownloadRequest.newBuilder()
                        .setFilePath(remotePath)
                        .build());

        String uploadId = init.getUploadId();
        String[] addrParts = init.getDataNodeAddress().split(":");
        String host = addrParts[0];
        int port = Integer.parseInt(addrParts[1]);
        long fileSize = init.getFileSize();

        ManagedChannel dataNodeChannel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
        DataNodeServiceGrpc.DataNodeServiceBlockingStub dataNodeStub =
                DataNodeServiceGrpc.newBlockingStub(dataNodeChannel);

        ProgressBar progressBar = new ProgressBar(fileSize);

        Path target = Path.of(localPath);
        Files.deleteIfExists(target);
        Files.createFile(target);

        try (FileChannel channel = FileChannel.open(target,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            dataNodeStub.downloadFileStream(DownloadRequest.newBuilder()
                            .setUploadId(uploadId)
                            .build())
                    .forEachRemaining(chunk -> {
                        try {
                            byte[] data = chunk.getData().toByteArray();
                            channel.write(ByteBuffer.wrap(data));
                            progressBar.update(channel.position());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        dataNodeChannel.shutdown();
        coordChannel.shutdown();
    }

}
