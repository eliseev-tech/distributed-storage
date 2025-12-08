package ru.eliseevtech.storage.datanode.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.eliseevtech.storage.datanode.model.UploadStats;
import ru.eliseevtech.storage.datanode.service.FileStorageService.ChunkConsumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FileStorageServiceTest {

    @TempDir
    Path tempDir;

    @Mock
    private DatanodeProperties properties;

    @Mock
    private DatanodeProperties.StorageProperties storageProperties;

    private FileStorageService fileStorageService;

    @BeforeEach
    void setUp() {
        fileStorageService = new FileStorageService(properties);
    }

    private void stubStoragePath() {
        when(properties.getStorage()).thenReturn(storageProperties);
        when(storageProperties.getPath()).thenReturn(tempDir.toString());
    }

    @Test
    void appendChunkShouldWriteDataAndUpdateStatsAndStreamChunksShouldReadSameData() throws IOException {
        stubStoragePath();

        String uploadId = "upload-1";
        byte[] data = "hello distributed storage".getBytes();

        // разобьём данные на чанки по 5 байт и запишем
        int chunkSizeForWrite = 5;
        int index = 0;
        for (int offset = 0; offset < data.length; offset += chunkSizeForWrite) {
            int end = Math.min(offset + chunkSizeForWrite, data.length);
            byte[] chunk = new byte[end - offset];
            System.arraycopy(data, offset, chunk, 0, end - offset);
            fileStorageService.appendChunk(uploadId, index++, chunk);
        }

        // проверяем, что файл действительно содержит эти данные
        Path uploadDir = tempDir.resolve(uploadId);
        Path filePath = uploadDir.resolve("file.bin");
        assertThat(Files.exists(filePath)).isTrue();

        byte[] fromDisk = Files.readAllBytes(filePath);
        assertThat(fromDisk).isEqualTo(data);

        // проверяем статистику
        UploadStats stats = fileStorageService.getUploadStats(uploadId);
        int expectedChunks = (int) Math.ceil(data.length / (double) chunkSizeForWrite);

        assertThat(stats.getUploadId()).isEqualTo(uploadId);
        assertThat(stats.getChunksCount()).isEqualTo(expectedChunks);
        assertThat(stats.getBytesWritten()).isEqualTo(data.length);

        // проверяем streamChunks
        int chunkSizeForRead = 4;
        List<Integer> indices = new ArrayList<>();
        List<Boolean> lastFlags = new ArrayList<>();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ChunkConsumer consumer = (chunkIndex, chunkData, isLast) -> {
            indices.add(chunkIndex);
            lastFlags.add(isLast);
            try {
                out.write(chunkData);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        fileStorageService.streamChunks(uploadId, chunkSizeForRead, consumer);

        byte[] streamed = out.toByteArray();
        assertThat(streamed).isEqualTo(data);

        // индексы чанков должны идти по возрастанию с 0
        assertThat(indices).isNotEmpty();
        for (int i = 0; i < indices.size(); i++) {
            assertThat(indices.get(i)).isEqualTo(i);
        }

        // флаг isLast должен быть true только у последнего чанка
        assertThat(lastFlags).isNotEmpty();
        for (int i = 0; i < lastFlags.size() - 1; i++) {
            assertThat(lastFlags.get(i)).isFalse();
        }
        assertThat(lastFlags.get(lastFlags.size() - 1)).isTrue();
    }

    @Test
    void deleteUploadShouldRemoveDirectoryAndResetStats() {
        stubStoragePath();

        String uploadId = "upload-2";

        // создаём данные
        fileStorageService.appendChunk(uploadId, 0, "test-data".getBytes());

        Path uploadDir = tempDir.resolve(uploadId);
        assertThat(Files.exists(uploadDir)).isTrue();

        UploadStats before = fileStorageService.getUploadStats(uploadId);
        assertThat(before.getBytesWritten()).isGreaterThan(0);
        assertThat(before.getChunksCount()).isGreaterThan(0);

        // удаляем
        fileStorageService.deleteUpload(uploadId);

        // каталог должен исчезнуть
        assertThat(Files.exists(uploadDir)).isFalse();

        // статистика должна быть сброшена
        UploadStats after = fileStorageService.getUploadStats(uploadId);
        assertThat(after.getUploadId()).isEqualTo(uploadId);
        assertThat(after.getChunksCount()).isZero();
        assertThat(after.getBytesWritten()).isZero();
    }

    @Test
    void getUploadStatsForUnknownUploadIdShouldReturnZeroValues() {
        String uploadId = "unknown";

        UploadStats stats = fileStorageService.getUploadStats(uploadId);

        assertThat(stats.getUploadId()).isEqualTo(uploadId);
        assertThat(stats.getChunksCount()).isZero();
        assertThat(stats.getBytesWritten()).isZero();
    }

}
