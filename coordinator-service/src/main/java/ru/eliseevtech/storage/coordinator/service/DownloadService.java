package ru.eliseevtech.storage.coordinator.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.eliseevtech.storage.coordinator.model.FileMetadata;
import ru.eliseevtech.storage.coordinator.model.FileStatus;
import ru.eliseevtech.storage.coordinator.storage.JsonFileMetadataStore;

@Service
@RequiredArgsConstructor
public class DownloadService {

    private final JsonFileMetadataStore metadataStore;

    public DownloadInitResult initiateDownload(String filePath) {
        FileMetadata meta = metadataStore.findByFilePath(filePath)
                .orElseThrow(() -> new IllegalArgumentException("File not found: " + filePath));

        if (meta.getStatus() != FileStatus.FINALIZED) {
            throw new IllegalStateException("File is not finalized");
        }

        return DownloadInitResult.builder()
                .uploadId(meta.getUploadId())
                .dataNodeAddress(meta.getDataNodeAddress())
                .fileSize(meta.getFileSize())
                .build();
    }

}
