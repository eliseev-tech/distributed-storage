package ru.eliseevtech.storage.coordinator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.eliseevtech.storage.coordinator.client.DataNodeControlClient;
import ru.eliseevtech.storage.coordinator.config.CoordinatorProperties;
import ru.eliseevtech.storage.coordinator.model.FileMetadata;
import ru.eliseevtech.storage.coordinator.storage.JsonFileMetadataStore;

@Slf4j
@Service
@RequiredArgsConstructor
public class CleanupService {

    private final JsonFileMetadataStore metadataStore;
    private final CoordinatorProperties properties;
    private final DataNodeControlClient dataNodeControlClient;

    @Scheduled(fixedDelayString = "${coordinator.cleanup.interval-ms}")
    public void cleanup() {
        long now = System.currentTimeMillis();
        long deadline = now - properties.getCleanup().getTimeoutMs();
        for (FileMetadata meta : metadataStore.findUploadingOlderThan(deadline)) {
            log.info("Cleaning up stale upload: {}", meta.getUploadId());
            String[] parts = meta.getDataNodeAddress().split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            try {
                dataNodeControlClient.deleteUpload(host, port, meta.getUploadId());
            } catch (Exception e) {
                log.warn("Failed to delete upload {} on datanode", meta.getUploadId(), e);
            }
            metadataStore.delete(meta);
        }
    }

}
