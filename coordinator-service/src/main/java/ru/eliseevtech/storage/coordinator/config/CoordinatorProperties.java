package ru.eliseevtech.storage.coordinator.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "coordinator")
public class CoordinatorProperties {

    private StorageProperties storage = new StorageProperties();
    private CleanupProperties cleanup = new CleanupProperties();
    private int chunkSize = 1048576;

    @Data
    public static class StorageProperties {
        private String path;
    }

    @Data
    public static class CleanupProperties {
        private long intervalMs;
        private long timeoutMs;
    }

}
