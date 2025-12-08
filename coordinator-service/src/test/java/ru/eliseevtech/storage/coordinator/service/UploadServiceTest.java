package ru.eliseevtech.storage.coordinator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.eliseevtech.storage.coordinator.client.DataNodeControlClient;
import ru.eliseevtech.storage.coordinator.config.CoordinatorProperties;
import ru.eliseevtech.storage.coordinator.model.DataNodeInfo;
import ru.eliseevtech.storage.coordinator.model.FileMetadata;
import ru.eliseevtech.storage.coordinator.model.FileStatus;
import ru.eliseevtech.storage.coordinator.registry.DataNodeRegistry;
import ru.eliseevtech.storage.coordinator.storage.JsonFileMetadataStore;
import ru.eliseevtech.storage.datanode.proto.GetUploadStatsResponse;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UploadServiceTest {

    @Mock
    private JsonFileMetadataStore metadataStore;

    @Mock
    private DataNodeRegistry dataNodeRegistry;

    @Mock
    private DataNodeControlClient dataNodeControlClient;

    private CoordinatorProperties properties;

    private UploadService uploadService;

    @BeforeEach
    void setUp() {
        properties = new CoordinatorProperties();
        properties.setChunkSize(1024);
        uploadService = new UploadService(metadataStore, dataNodeRegistry, properties, dataNodeControlClient);
    }

    @Test
    void initiateUploadShouldCreateNewMetadataWhenNoExistingAndResumeFalse() {
        String filePath = "/remote/test.txt";
        long fileSize = 100L;

        when(metadataStore.findByFilePath(filePath)).thenReturn(Optional.empty());

        DataNodeInfo node = mock(DataNodeInfo.class);
        when(node.getHost()).thenReturn("datanode1");
        when(node.getPort()).thenReturn(50051);
        when(dataNodeRegistry.chooseNodeForUpload()).thenReturn(Optional.of(node));

        ArgumentCaptor<FileMetadata> metaCaptor = ArgumentCaptor.forClass(FileMetadata.class);

        InitiateUploadResult result = uploadService.initiateUpload(filePath, fileSize, false);

        assertThat(result.isResumed()).isFalse();
        assertThat(result.getChunkSize()).isEqualTo(1024);
        assertThat(result.getDataNodeAddress()).isEqualTo("datanode1:50051");
        assertThat(result.getUploadId()).isNotBlank();
        assertThat(result.getLastChunkIndex()).isEqualTo(-1);
        assertThat(result.getBytesUploaded()).isZero();

        verify(metadataStore).save(metaCaptor.capture());
        FileMetadata saved = metaCaptor.getValue();

        assertThat(saved.getFilePath()).isEqualTo(filePath);
        assertThat(saved.getFileSize()).isEqualTo(fileSize);
        assertThat(saved.getStatus()).isEqualTo(FileStatus.UPLOADING);
        assertThat(saved.getUploadId()).isEqualTo(result.getUploadId());
        assertThat(saved.getDataNodeAddress()).isEqualTo("datanode1:50051");
        assertThat(saved.getLastChunkIndex()).isEqualTo(-1);
        assertThat(saved.getBytesUploaded()).isZero();
        assertThat(saved.getCreatedAt()).isGreaterThan(0L);
    }

    @Test
    void initiateUploadWithResumeShouldReturnExistingUploadingMetadata() {
        String filePath = "/remote/test.txt";

        FileMetadata existing = FileMetadata.builder()
                .uploadId("upload-1")
                .filePath(filePath)
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(200L)
                .createdAt(1_000L)
                .lastChunkIndex(10)
                .bytesUploaded(10240L)
                .build();

        when(metadataStore.findByFilePath(filePath)).thenReturn(Optional.of(existing));

        InitiateUploadResult result = uploadService.initiateUpload(filePath, 200L, true);

        assertThat(result.isResumed()).isTrue();
        assertThat(result.getUploadId()).isEqualTo("upload-1");
        assertThat(result.getDataNodeAddress()).isEqualTo("datanode1:50051");
        assertThat(result.getChunkSize()).isEqualTo(1024);
        assertThat(result.getLastChunkIndex()).isEqualTo(10);
        assertThat(result.getBytesUploaded()).isEqualTo(10240L);

        verify(dataNodeRegistry, never()).chooseNodeForUpload();
        verify(metadataStore, never()).save(any());
    }

    @Test
    void initiateUploadShouldFailWhenFileAlreadyFinalized() {
        String filePath = "/remote/test.txt";

        FileMetadata existing = FileMetadata.builder()
                .uploadId("upload-1")
                .filePath(filePath)
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.FINALIZED)
                .fileSize(200L)
                .createdAt(1_000L)
                .build();

        when(metadataStore.findByFilePath(filePath)).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> uploadService.initiateUpload(filePath, 200L, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("File already exists and finalized");

        verifyNoInteractions(dataNodeRegistry);
    }

    @Test
    void initiateUploadShouldFailWhenNoDataNodesAvailable() {
        String filePath = "/remote/test.txt";

        when(metadataStore.findByFilePath(filePath)).thenReturn(Optional.empty());
        when(dataNodeRegistry.chooseNodeForUpload()).thenReturn(Optional.empty());

        assertThatThrownBy(() -> uploadService.initiateUpload(filePath, 100L, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No available data nodes");
    }

    @Test
    void updateUploadProgressShouldUpdateMetadataAndPersist() {
        String uploadId = "upload-1";

        FileMetadata existing = FileMetadata.builder()
                .uploadId(uploadId)
                .filePath("/remote/test.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(200L)
                .createdAt(1_000L)
                .lastChunkIndex(0)
                .bytesUploaded(0L)
                .build();

        when(metadataStore.findByUploadId(uploadId)).thenReturn(Optional.of(existing));

        uploadService.updateUploadProgress(uploadId, 5, 10240L);

        ArgumentCaptor<FileMetadata> metaCaptor = ArgumentCaptor.forClass(FileMetadata.class);
        verify(metadataStore).save(metaCaptor.capture());

        FileMetadata saved = metaCaptor.getValue();
        assertThat(saved.getLastChunkIndex()).isEqualTo(5);
        assertThat(saved.getBytesUploaded()).isEqualTo(10240L);
    }

    @Test
    void finalizeUploadShouldSetFinalizedStatusWhenSizesMatch() {
        String uploadId = "upload-1";
        String filePath = "/remote/test.txt";

        FileMetadata existing = FileMetadata.builder()
                .uploadId(uploadId)
                .filePath(filePath)
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(100L)
                .createdAt(1_000L)
                .lastChunkIndex(-1)
                .bytesUploaded(0L)
                .build();

        when(metadataStore.findByUploadId(uploadId)).thenReturn(Optional.of(existing));

        GetUploadStatsResponse stats = GetUploadStatsResponse.newBuilder()
                .setBytesWritten(100L)
                .setChunksCount(3)
                .build();

        when(dataNodeControlClient.getUploadStats("datanode1", 50051, uploadId))
                .thenReturn(stats);

        uploadService.finalizeUpload(uploadId, filePath);

        ArgumentCaptor<FileMetadata> metaCaptor = ArgumentCaptor.forClass(FileMetadata.class);
        verify(metadataStore).save(metaCaptor.capture());

        FileMetadata saved = metaCaptor.getValue();
        assertThat(saved.getStatus()).isEqualTo(FileStatus.FINALIZED);
        assertThat(saved.getBytesUploaded()).isEqualTo(100L);
        assertThat(saved.getLastChunkIndex()).isEqualTo(2);
        assertThat(saved.getFinalizedAt()).isNotNull();
        assertThat(saved.getFinalizedAt()).isGreaterThan(0L);
    }

    @Test
    void finalizeUploadShouldFailOnFileSizeMismatch() {
        String uploadId = "upload-1";
        String filePath = "/remote/test.txt";

        FileMetadata existing = FileMetadata.builder()
                .uploadId(uploadId)
                .filePath(filePath)
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(200L)
                .createdAt(1_000L)
                .build();

        when(metadataStore.findByUploadId(uploadId)).thenReturn(Optional.of(existing));

        GetUploadStatsResponse stats = GetUploadStatsResponse.newBuilder()
                .setBytesWritten(150L)
                .setChunksCount(3)
                .build();

        when(dataNodeControlClient.getUploadStats("datanode1", 50051, uploadId))
                .thenReturn(stats);

        assertThatThrownBy(() -> uploadService.finalizeUpload(uploadId, filePath))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("File size mismatch");

        verify(metadataStore, never()).save(any());
    }

    @Test
    void getUploadStatusShouldReturnDataFromMetadata() {
        String uploadId = "upload-1";

        FileMetadata existing = FileMetadata.builder()
                .uploadId(uploadId)
                .filePath("/remote/test.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(200L)
                .createdAt(1_000L)
                .lastChunkIndex(5)
                .bytesUploaded(10240L)
                .build();

        when(metadataStore.findByUploadId(uploadId)).thenReturn(Optional.of(existing));

        GetUploadStatusResult status = uploadService.getUploadStatus(uploadId);

        assertThat(status.getUploadId()).isEqualTo(uploadId);
        assertThat(status.getStatus()).isEqualTo("UPLOADING");
        assertThat(status.getLastChunkIndex()).isEqualTo(5);
        assertThat(status.getBytesUploaded()).isEqualTo(10240L);
    }

}
