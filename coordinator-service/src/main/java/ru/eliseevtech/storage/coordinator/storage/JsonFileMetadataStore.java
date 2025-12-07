package ru.eliseevtech.storage.coordinator.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.eliseevtech.storage.coordinator.model.FileMetadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class JsonFileMetadataStore {

    private final Path storePath;
    private final ObjectMapper objectMapper;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<String, FileMetadata> byFilePath = new HashMap<>();
    private final Map<String, FileMetadata> byUploadId = new HashMap<>();

    public JsonFileMetadataStore(Path storePath, ObjectMapper objectMapper) {
        this.storePath = storePath;
        this.objectMapper = objectMapper;
        loadFromDisk();
    }

    public Optional<FileMetadata> findByFilePath(String filePath) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(byFilePath.get(filePath));
        } finally {
            lock.readLock().unlock();
        }
    }

    public Optional<FileMetadata> findByUploadId(String uploadId) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(byUploadId.get(uploadId));
        } finally {
            lock.readLock().unlock();
        }
    }

    public void save(FileMetadata metadata) {
        lock.writeLock().lock();
        try {
            byFilePath.put(metadata.getFilePath(), metadata);
            byUploadId.put(metadata.getUploadId(), metadata);
            persist();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void delete(FileMetadata metadata) {
        lock.writeLock().lock();
        try {
            byFilePath.remove(metadata.getFilePath());
            byUploadId.remove(metadata.getUploadId());
            persist();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<FileMetadata> findUploadingOlderThan(long deadlineMillis) {
        lock.readLock().lock();
        try {
            List<FileMetadata> result = new ArrayList<>();
            for (FileMetadata meta : byUploadId.values()) {
                if (meta.getStatus() != null &&
                        meta.getStatus().name().equals("UPLOADING") &&
                        meta.getCreatedAt() < deadlineMillis) {
                    result.add(meta);
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<FileMetadata> findAll() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(byUploadId.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    private void loadFromDisk() {
        lock.writeLock().lock();
        try {
            if (!Files.exists(storePath)) {
                Files.createDirectories(storePath.getParent());
                Files.createFile(storePath);
                persist();
                return;
            }
            byte[] bytes = Files.readAllBytes(storePath);
            if (bytes.length == 0) {
                return;
            }
            List<FileMetadata> list = objectMapper.readValue(
                    bytes, new TypeReference<>() {
                    });
            byFilePath.clear();
            byUploadId.clear();
            for (FileMetadata meta : list) {
                byFilePath.put(meta.getFilePath(), meta);
                byUploadId.put(meta.getUploadId(), meta);
            }
        } catch (IOException e) {
            log.error("Failed to load metadata from disk", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void persist() {
        try {
            List<FileMetadata> list = new ArrayList<>(byUploadId.values());
            byte[] bytes = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsBytes(list);
            Files.write(storePath, bytes, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("Failed to persist metadata to disk", e);
        }
    }

}
