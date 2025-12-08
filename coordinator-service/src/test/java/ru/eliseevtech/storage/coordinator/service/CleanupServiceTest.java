package ru.eliseevtech.storage.coordinator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.eliseevtech.storage.coordinator.client.DataNodeControlClient;
import ru.eliseevtech.storage.coordinator.config.CoordinatorProperties;
import ru.eliseevtech.storage.coordinator.model.FileMetadata;
import ru.eliseevtech.storage.coordinator.model.FileStatus;
import ru.eliseevtech.storage.coordinator.storage.JsonFileMetadataStore;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CleanupServiceTest {

    @Mock
    private JsonFileMetadataStore metadataStore;

    @Mock
    private DataNodeControlClient dataNodeControlClient;

    private CoordinatorProperties properties;

    private CleanupService cleanupService;

    @BeforeEach
    void setUp() {
        properties = new CoordinatorProperties();
        CoordinatorProperties.CleanupProperties cleanupProps = new CoordinatorProperties.CleanupProperties();
        cleanupProps.setTimeoutMs(600_000L);
        properties.setCleanup(cleanupProps);

        cleanupService = new CleanupService(metadataStore, properties, dataNodeControlClient);
    }

    @Test
    void cleanupShouldCallDeleteUploadAndRemoveMetadataForAllStaleUploads() {
        FileMetadata meta1 = FileMetadata.builder()
                .uploadId("u1")
                .filePath("/remote/1.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .createdAt(1_000L)
                .build();

        FileMetadata meta2 = FileMetadata.builder()
                .uploadId("u2")
                .filePath("/remote/2.txt")
                .dataNodeAddress("datanode2:50052")
                .status(FileStatus.UPLOADING)
                .createdAt(2_000L)
                .build();

        when(metadataStore.findUploadingOlderThan(anyLong()))
                .thenReturn(List.of(meta1, meta2));

        cleanupService.cleanup();

        verify(dataNodeControlClient).deleteUpload("datanode1", 50051, "u1");
        verify(dataNodeControlClient).deleteUpload("datanode2", 50052, "u2");

        verify(metadataStore).delete(meta1);
        verify(metadataStore).delete(meta2);
    }

    @Test
    void cleanupShouldSwallowExceptionsFromDataNodeAndStillDeleteMetadata() {
        FileMetadata meta = FileMetadata.builder()
                .uploadId("u1")
                .filePath("/remote/1.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .createdAt(1_000L)
                .build();

        when(metadataStore.findUploadingOlderThan(anyLong()))
                .thenReturn(List.of(meta));

        doThrow(new RuntimeException("boom"))
                .when(dataNodeControlClient)
                .deleteUpload("datanode1", 50051, "u1");

        assertThatCode(() -> cleanupService.cleanup())
                .doesNotThrowAnyException();

        verify(metadataStore).delete(meta);
    }

}
