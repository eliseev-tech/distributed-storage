package ru.eliseevtech.storage.datanode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import ru.eliseevtech.storage.datanode.model.UploadStats;
import ru.eliseevtech.storage.datanode.service.DatanodeProperties;
import ru.eliseevtech.storage.datanode.service.FileStorageService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = FileStorageServiceIntegrationTest.TestConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileStorageServiceIntegrationTest {

    @Autowired
    private FileStorageService fileStorageService;

    private Path getBaseDir() {
        String base = System.getProperty("java.io.tmpdir");
        return Path.of(base).resolve("datanode-it");
    }

    @AfterEach
    void tearDown() throws IOException {
        Path baseDir = getBaseDir();
        if (Files.exists(baseDir)) {
            Files.walk(baseDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {
                        }
                    });
        }
    }

    @Test
    void uploadAndDownload_shouldTransferDataCorrectly() {
        String uploadId = UUID.randomUUID().toString();
        byte[] originalData = "Hello gRPC data node!".getBytes(StandardCharsets.UTF_8);

        byte[] chunk1 = new byte[10];
        byte[] chunk2 = new byte[originalData.length - 10];
        System.arraycopy(originalData, 0, chunk1, 0, 10);
        System.arraycopy(originalData, 10, chunk2, 0, originalData.length - 10);

        fileStorageService.appendChunk(uploadId, 0, chunk1);
        fileStorageService.appendChunk(uploadId, 1, chunk2);

        UploadStats stats = fileStorageService.getUploadStats(uploadId);
        assertThat(stats.getUploadId()).isEqualTo(uploadId);
        assertThat(stats.getChunksCount()).isEqualTo(2);
        assertThat(stats.getBytesWritten()).isEqualTo(originalData.length);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        fileStorageService.streamChunks(uploadId, 8, (index, data, isLast) -> {
            try {
                out.write(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        byte[] downloaded = out.toByteArray();

        assertThat(downloaded).containsExactly(originalData);

        fileStorageService.deleteUpload(uploadId);
        UploadStats afterDelete = fileStorageService.getUploadStats(uploadId);
        assertThat(afterDelete.getBytesWritten()).isZero();
        assertThat(afterDelete.getChunksCount()).isZero();
    }

    @Configuration
    static class TestConfig {

        @Bean
        public DatanodeProperties datanodeProperties() {
            DatanodeProperties props = new DatanodeProperties();
            props.getStorage().setPath(System.getProperty("java.io.tmpdir") + "/datanode-it");
            props.getGrpc().setPort(0);
            return props;
        }

        @Bean
        public FileStorageService fileStorageService(DatanodeProperties props) {
            return new FileStorageService(props);
        }
    }

}
