package ru.eliseevtech.storage.coordinator.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.eliseevtech.storage.coordinator.model.FileMetadata;
import ru.eliseevtech.storage.coordinator.model.FileStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class JsonFileMetadataStoreTest {

    @TempDir
    Path tempDir;

    private ObjectMapper objectMapper() {
        return JsonMapper.builder().findAndAddModules().build();
    }

    @Test
    void constructorShouldCreateFileIfNotExistsAndLoadEmptyState() throws IOException {
        Path storePath = tempDir.resolve("metadata.json");
        assertThat(Files.exists(storePath)).isFalse();

        JsonFileMetadataStore store = new JsonFileMetadataStore(storePath, objectMapper());

        assertThat(Files.exists(storePath)).isTrue();
        assertThat(Files.readAllBytes(storePath)).isNotNull();
        assertThat(store.findAll()).isEmpty();
    }

    @Test
    void saveShouldAllowFindingByFilePathAndUploadId() {
        Path storePath = tempDir.resolve("metadata.json");
        JsonFileMetadataStore store = new JsonFileMetadataStore(storePath, objectMapper());

        FileMetadata metadata = FileMetadata.builder()
                .uploadId("upload-1")
                .filePath("/remote/test.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(123L)
                .createdAt(1000L)
                .bytesUploaded(0L)
                .lastChunkIndex(-1)
                .build();

        store.save(metadata);

        Optional<FileMetadata> byPath = store.findByFilePath("/remote/test.txt");
        Optional<FileMetadata> byId = store.findByUploadId("upload-1");

        assertThat(byPath).isPresent();
        assertThat(byPath.get().getUploadId()).isEqualTo("upload-1");

        assertThat(byId).isPresent();
        assertThat(byId.get().getFilePath()).isEqualTo("/remote/test.txt");

        assertThat(store.findAll()).hasSize(1);
    }

    @Test
    void saveShouldOverrideExistingRecordForSameKeys() {
        Path storePath = tempDir.resolve("metadata.json");
        JsonFileMetadataStore store = new JsonFileMetadataStore(storePath, objectMapper());

        FileMetadata v1 = FileMetadata.builder()
                .uploadId("upload-1")
                .filePath("/remote/test.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(100L)
                .createdAt(1000L)
                .bytesUploaded(0L)
                .lastChunkIndex(-1)
                .build();

        store.save(v1);

        FileMetadata v2 = v1.toBuilder()
                .status(FileStatus.FINALIZED)
                .bytesUploaded(100L)
                .lastChunkIndex(4)
                .build();

        store.save(v2);

        List<FileMetadata> all = store.findAll();
        assertThat(all).hasSize(1);
        FileMetadata saved = all.get(0);
        assertThat(saved.getStatus()).isEqualTo(FileStatus.FINALIZED);
        assertThat(saved.getBytesUploaded()).isEqualTo(100L);
        assertThat(saved.getLastChunkIndex()).isEqualTo(4);
    }

    @Test
    void deleteShouldRemoveFromBothIndexesAndPersist() throws IOException {
        Path storePath = tempDir.resolve("metadata.json");
        JsonFileMetadataStore store = new JsonFileMetadataStore(storePath, objectMapper());

        FileMetadata metadata = FileMetadata.builder()
                .uploadId("upload-1")
                .filePath("/remote/test.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(100L)
                .createdAt(1000L)
                .bytesUploaded(0L)
                .lastChunkIndex(-1)
                .build();

        store.save(metadata);
        assertThat(store.findAll()).hasSize(1);

        store.delete(metadata);

        assertThat(store.findAll()).isEmpty();
        assertThat(store.findByFilePath("/remote/test.txt")).isEmpty();
        assertThat(store.findByUploadId("upload-1")).isEmpty();

        String jsonOnDisk = Files.readString(storePath);
        assertThat(jsonOnDisk).contains("[ ]").doesNotContain("upload-1");
    }

    @Test
    void dataShouldBeLoadedFromDiskOnRecreation() {
        Path storePath = tempDir.resolve("metadata.json");
        JsonFileMetadataStore store1 = new JsonFileMetadataStore(storePath, objectMapper());

        FileMetadata metadata = FileMetadata.builder()
                .uploadId("upload-1")
                .filePath("/remote/test.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.FINALIZED)
                .fileSize(42L)
                .createdAt(1000L)
                .bytesUploaded(42L)
                .lastChunkIndex(0)
                .build();

        store1.save(metadata);

        JsonFileMetadataStore store2 = new JsonFileMetadataStore(storePath, objectMapper());

        Optional<FileMetadata> byPath = store2.findByFilePath("/remote/test.txt");
        assertThat(byPath).isPresent();
        assertThat(byPath.get().getUploadId()).isEqualTo("upload-1");
        assertThat(byPath.get().getStatus()).isEqualTo(FileStatus.FINALIZED);
    }

    @Test
    void findUploadingOlderThanShouldReturnOnlyMatchingEntries() {
        Path storePath = tempDir.resolve("metadata.json");
        JsonFileMetadataStore store = new JsonFileMetadataStore(storePath, objectMapper());

        long now = 1_000_000L;
        long tooOld = now - 10_000L;
        long fresh = now - 1_000L;

        FileMetadata oldUploading = FileMetadata.builder()
                .uploadId("old-uploading")
                .filePath("/remote/old-uploading.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(10L)
                .createdAt(tooOld)
                .build();

        FileMetadata freshUploading = FileMetadata.builder()
                .uploadId("fresh-uploading")
                .filePath("/remote/fresh-uploading.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.UPLOADING)
                .fileSize(10L)
                .createdAt(fresh)
                .build();

        FileMetadata finalized = FileMetadata.builder()
                .uploadId("finalized")
                .filePath("/remote/finalized.txt")
                .dataNodeAddress("datanode1:50051")
                .status(FileStatus.FINALIZED)
                .fileSize(10L)
                .createdAt(tooOld)
                .build();

        store.save(oldUploading);
        store.save(freshUploading);
        store.save(finalized);

        long deadline = now - 5_000L;

        List<FileMetadata> result = store.findUploadingOlderThan(deadline);

        assertThat(result)
                .hasSize(1)
                .first()
                .extracting(FileMetadata::getUploadId)
                .isEqualTo("old-uploading");
    }

}
