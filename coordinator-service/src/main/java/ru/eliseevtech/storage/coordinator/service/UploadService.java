package ru.eliseevtech.storage.coordinator.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.eliseevtech.storage.coordinator.client.DataNodeControlClient;
import ru.eliseevtech.storage.coordinator.config.CoordinatorProperties;
import ru.eliseevtech.storage.coordinator.model.DataNodeInfo;
import ru.eliseevtech.storage.coordinator.model.FileMetadata;
import ru.eliseevtech.storage.coordinator.model.FileStatus;
import ru.eliseevtech.storage.coordinator.registry.DataNodeRegistry;
import ru.eliseevtech.storage.coordinator.storage.JsonFileMetadataStore;
import ru.eliseevtech.storage.datanode.proto.GetUploadStatsResponse;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class UploadService {

    private final JsonFileMetadataStore metadataStore;
    private final DataNodeRegistry dataNodeRegistry;
    private final CoordinatorProperties properties;
    private final DataNodeControlClient dataNodeControlClient;

    public InitiateUploadResult initiateUpload(String filePath, long fileSize, boolean resume) {
        Optional<FileMetadata> existing = metadataStore.findByFilePath(filePath);
        if (resume && existing.isPresent()
                && existing.get().getStatus() == FileStatus.UPLOADING) {
            FileMetadata meta = existing.get();
            return InitiateUploadResult.builder()
                    .uploadId(meta.getUploadId())
                    .dataNodeAddress(meta.getDataNodeAddress())
                    .chunkSize(properties.getChunkSize())
                    .resumed(true)
                    .lastChunkIndex(meta.getLastChunkIndex())
                    .bytesUploaded(meta.getBytesUploaded())
                    .build();
        }

        if (existing.isPresent() && existing.get().getStatus() == FileStatus.FINALIZED) {
            throw new IllegalStateException("File already exists and finalized for path: " + filePath);
        }

        DataNodeInfo node = dataNodeRegistry.chooseNodeForUpload()
                .orElseThrow(() -> new IllegalStateException("No available data nodes"));

        String uploadId = UUID.randomUUID().toString();
        long now = Instant.now().toEpochMilli();

        FileMetadata meta = FileMetadata.builder()
                .filePath(filePath)
                .uploadId(uploadId)
                .dataNodeAddress(node.getHost() + ":" + node.getPort())
                .status(FileStatus.UPLOADING)
                .fileSize(fileSize)
                .createdAt(now)
                .lastChunkIndex(-1)
                .bytesUploaded(0L)
                .build();
        metadataStore.save(meta);

        return InitiateUploadResult.builder()
                .uploadId(uploadId)
                .dataNodeAddress(meta.getDataNodeAddress())
                .chunkSize(properties.getChunkSize())
                .resumed(false)
                .lastChunkIndex(-1)
                .bytesUploaded(0L)
                .build();
    }

    public void updateUploadProgress(String uploadId, int lastChunkIndex, long bytesUploaded) {
        FileMetadata meta = metadataStore.findByUploadId(uploadId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown uploadId: " + uploadId));
        meta.setLastChunkIndex(lastChunkIndex);
        meta.setBytesUploaded(bytesUploaded);
        metadataStore.save(meta);
    }

    public void finalizeUpload(String uploadId, String filePath) {
        FileMetadata meta = metadataStore.findByUploadId(uploadId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown uploadId: " + uploadId));

        if (!meta.getFilePath().equals(filePath)) {
            throw new IllegalArgumentException("File path mismatch");
        }

        String[] parts = meta.getDataNodeAddress().split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        GetUploadStatsResponse stats = dataNodeControlClient.getUploadStats(host, port, uploadId);

        if (stats.getBytesWritten() != meta.getFileSize()) {
            throw new IllegalStateException("File size mismatch: expected " +
                    meta.getFileSize() + ", actual " + stats.getBytesWritten());
        }

        meta.setStatus(FileStatus.FINALIZED);
        meta.setFinalizedAt(Instant.now().toEpochMilli());
        meta.setLastChunkIndex(stats.getChunksCount() - 1);
        meta.setBytesUploaded(stats.getBytesWritten());
        metadataStore.save(meta);
    }

    public GetUploadStatusResult getUploadStatus(String uploadId) {
        FileMetadata meta = metadataStore.findByUploadId(uploadId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown uploadId: " + uploadId));
        return GetUploadStatusResult.builder()
                .uploadId(meta.getUploadId())
                .status(meta.getStatus().name())
                .bytesUploaded(meta.getBytesUploaded())
                .lastChunkIndex(meta.getLastChunkIndex())
                .build();
    }

}
