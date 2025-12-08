package ru.eliseevtech.storage.coordinator.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.eliseevtech.storage.coordinator.model.FileMetadata;
import ru.eliseevtech.storage.coordinator.model.FileStatus;
import ru.eliseevtech.storage.coordinator.storage.JsonFileMetadataStore;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DownloadServiceTest {

    @Mock
    private JsonFileMetadataStore metadataStore;

    @Test
    void initiateDownloadShouldReturnInfoForFinalizedFile() {
        String filePath = "/remote/test.txt";

        FileMetadata meta = FileMetadata.builder()
                .uploadId("upload-1")
                .filePath(filePath)
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.FINALIZED)
                .fileSize(123L)
                .createdAt(1_000L)
                .finalizedAt(2_000L)
                .lastChunkIndex(10)
                .bytesUploaded(123L)
                .build();

        when(metadataStore.findByFilePath(filePath)).thenReturn(Optional.of(meta));

        DownloadService service = new DownloadService(metadataStore);

        DownloadInitResult result = service.initiateDownload(filePath);

        assertThat(result.getUploadId()).isEqualTo("upload-1");
        assertThat(result.getDataNodeAddress()).isEqualTo("datanode1:50051");
        assertThat(result.getFileSize()).isEqualTo(123L);
    }

    @Test
    void initiateDownloadShouldFailWhenFileNotFound() {
        String filePath = "/remote/missing.txt";

        when(metadataStore.findByFilePath(filePath)).thenReturn(Optional.empty());

        DownloadService service = new DownloadService(metadataStore);

        assertThatThrownBy(() -> service.initiateDownload(filePath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("File not found: " + filePath);
    }

    @Test
    void initiateDownloadShouldFailWhenFileIsNotFinalized() {
        String filePath = "/remote/not-finalized.txt";

        FileMetadata meta = FileMetadata.builder()
                .uploadId("upload-2")
                .filePath(filePath)
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(100L)
                .createdAt(1_000L)
                .bytesUploaded(50L)
                .lastChunkIndex(4)
                .build();

        when(metadataStore.findByFilePath(filePath)).thenReturn(Optional.of(meta));

        DownloadService service = new DownloadService(metadataStore);

        assertThatThrownBy(() -> service.initiateDownload(filePath))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("File is not finalized");
    }

}
