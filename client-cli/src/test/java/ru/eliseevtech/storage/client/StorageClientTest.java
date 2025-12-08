package ru.eliseevtech.storage.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.eliseevtech.storage.coordinator.proto.CoordinatorServiceGrpc;
import ru.eliseevtech.storage.coordinator.proto.FinalizeUploadRequest;
import ru.eliseevtech.storage.coordinator.proto.FinalizeUploadResponse;
import ru.eliseevtech.storage.coordinator.proto.InitiateDownloadRequest;
import ru.eliseevtech.storage.coordinator.proto.InitiateDownloadResponse;
import ru.eliseevtech.storage.coordinator.proto.InitiateUploadRequest;
import ru.eliseevtech.storage.coordinator.proto.InitiateUploadResponse;
import ru.eliseevtech.storage.datanode.proto.DataNodeServiceGrpc;
import ru.eliseevtech.storage.datanode.proto.DownloadChunk;
import ru.eliseevtech.storage.datanode.proto.DownloadRequest;
import ru.eliseevtech.storage.datanode.proto.UploadChunk;
import ru.eliseevtech.storage.datanode.proto.UploadResponse;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StorageClientTest {

    @Mock
    private CoordinatorClientProperties properties;

    @Test
    void uploadShouldSendFileInChunksAndCallFinalize() throws Exception {
        byte[] data = "hello distributed storage".getBytes();
        Path tempFile = Files.createTempFile("upload-test", ".bin");
        Files.write(tempFile, data);

        when(properties.getHost()).thenReturn("localhost");
        when(properties.getPort()).thenReturn(50060);

        StorageClient client = new StorageClient(properties);

        int chunkSize = 4;
        String uploadId = "u1";
        String remotePath = "/remote/file.txt";
        String dataNodeAddress = "datanode1:50051";

        InitiateUploadResponse initResponse = InitiateUploadResponse.newBuilder()
                .setUploadId(uploadId)
                .setDataNodeAddress(dataNodeAddress)
                .setChunkSize(chunkSize)
                .setBytesUploaded(0L)
                .setLastChunkIndex(-1)
                .build();

        FinalizeUploadResponse finalizeResponse = FinalizeUploadResponse.newBuilder()
                .setSuccess(true)
                .build();

        CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordStub =
                mock(CoordinatorServiceGrpc.CoordinatorServiceBlockingStub.class);
        DataNodeServiceGrpc.DataNodeServiceStub dataNodeStub =
                mock(DataNodeServiceGrpc.DataNodeServiceStub.class);

        List<UploadChunk> sentChunks = new ArrayList<>();

        try (MockedStatic<CoordinatorServiceGrpc> coordStatic = mockStatic(CoordinatorServiceGrpc.class);
             MockedStatic<DataNodeServiceGrpc> dataNodeStatic = mockStatic(DataNodeServiceGrpc.class)) {

            coordStatic.when(() -> CoordinatorServiceGrpc.newBlockingStub(any(ManagedChannel.class)))
                    .thenReturn(coordStub);
            dataNodeStatic.when(() -> DataNodeServiceGrpc.newStub(any(ManagedChannel.class)))
                    .thenReturn(dataNodeStub);

            when(coordStub.initiateUpload(any(InitiateUploadRequest.class)))
                    .thenReturn(initResponse);
            when(coordStub.finalizeUpload(any(FinalizeUploadRequest.class)))
                    .thenReturn(finalizeResponse);

            when(dataNodeStub.uploadFileStream(any()))
                    .thenAnswer(invocation -> {
                        StreamObserver<UploadResponse> responseObserver = invocation.getArgument(0);

                        return new StreamObserver<UploadChunk>() {
                            @Override
                            public void onNext(UploadChunk value) {
                                sentChunks.add(value);
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onNext(UploadResponse.newBuilder()
                                        .setUploadId(uploadId)
                                        .setUploadedBytes(data.length)
                                        .setUploadedChunks(sentChunks.size())
                                        .setSuccess(true)
                                        .build());
                                responseObserver.onCompleted();
                            }
                        };
                    });

            client.upload(remotePath, tempFile.toString(), false);
        }

        // проверяем, что все байты ушли
        int totalBytes = sentChunks.stream()
                .mapToInt(c -> c.getData().size())
                .sum();
        assertThat(totalBytes).isEqualTo(data.length);

        // индексы чанков подряд, uploadId везде один
        assertThat(sentChunks).isNotEmpty();
        for (int i = 0; i < sentChunks.size(); i++) {
            UploadChunk c = sentChunks.get(i);
            assertThat(c.getChunkIndex()).isEqualTo(i);
            assertThat(c.getUploadId()).isEqualTo(uploadId);
        }

        // финализация с корректными параметрами
        verify(coordStub).finalizeUpload(argThat(req ->
                req.getUploadId().equals(uploadId) &&
                        req.getFilePath().equals(remotePath)
        ));
    }

    @Test
    void downloadShouldWriteAllChunksToLocalFile() throws Exception {
        byte[] data = "downloaded data from datanode".getBytes();
        String uploadId = "u-download";
        String remotePath = "/remote/download.txt";
        String dataNodeAddress = "datanode1:50051";

        when(properties.getHost()).thenReturn("localhost");
        when(properties.getPort()).thenReturn(50060);

        StorageClient client = new StorageClient(properties);

        InitiateDownloadResponse initResponse = InitiateDownloadResponse.newBuilder()
                .setUploadId(uploadId)
                .setDataNodeAddress(dataNodeAddress)
                .setFileSize(data.length)
                .build();

        CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordStub =
                mock(CoordinatorServiceGrpc.CoordinatorServiceBlockingStub.class);
        DataNodeServiceGrpc.DataNodeServiceBlockingStub dataNodeStub =
                mock(DataNodeServiceGrpc.DataNodeServiceBlockingStub.class);

        List<DownloadChunk> chunks = new ArrayList<>();
        int chunkSize = 5;
        int idx = 0;
        for (int offset = 0; offset < data.length; offset += chunkSize) {
            int end = Math.min(offset + chunkSize, data.length);
            byte[] part = new byte[end - offset];
            System.arraycopy(data, offset, part, 0, end - offset);

            boolean isLast = end >= data.length;

            DownloadChunk chunk = DownloadChunk.newBuilder()
                    .setChunkIndex(idx++)
                    .setData(ByteString.copyFrom(part))
                    .setIsLast(isLast)      // ВАЖНО: имя поля bool is_last → setIsLast(...)
                    .build();
            chunks.add(chunk);
        }

        try (MockedStatic<CoordinatorServiceGrpc> coordStatic = mockStatic(CoordinatorServiceGrpc.class);
             MockedStatic<DataNodeServiceGrpc> dataNodeStatic = mockStatic(DataNodeServiceGrpc.class)) {

            coordStatic.when(() -> CoordinatorServiceGrpc.newBlockingStub(any(ManagedChannel.class)))
                    .thenReturn(coordStub);
            dataNodeStatic.when(() -> DataNodeServiceGrpc.newBlockingStub(any(ManagedChannel.class)))
                    .thenReturn(dataNodeStub);

            when(coordStub.initiateDownload(any(InitiateDownloadRequest.class)))
                    .thenReturn(initResponse);

            when(dataNodeStub.downloadFileStream(any(DownloadRequest.class)))
                    .thenAnswer(invocation -> new Iterator<DownloadChunk>() {
                        private int i = 0;

                        @Override
                        public boolean hasNext() {
                            return i < chunks.size();
                        }

                        @Override
                        public DownloadChunk next() {
                            return chunks.get(i++);
                        }
                    });

            Path target = Files.createTempFile("download-test", ".bin");

            client.download(remotePath, target.toString());

            byte[] fromDisk;
            try (FileChannel ch = FileChannel.open(target, StandardOpenOption.READ)) {
                ByteBuffer buf = ByteBuffer.allocate(data.length);
                ch.read(buf);
                buf.flip();
                fromDisk = new byte[buf.remaining()];
                buf.get(fromDisk);
            }

            assertThat(fromDisk).isEqualTo(data);
        }
    }

}
