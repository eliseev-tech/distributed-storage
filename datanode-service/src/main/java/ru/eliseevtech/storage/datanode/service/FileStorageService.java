package ru.eliseevtech.storage.datanode.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.eliseevtech.storage.datanode.model.UploadStats;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class FileStorageService {

    private final DatanodeProperties properties;

    private final ConcurrentMap<String, UploadStats> statsMap = new ConcurrentHashMap<>();

    public synchronized void appendChunk(String uploadId, int chunkIndex, byte[] data) {
        Path dir = getUploadDir(uploadId);
        Path file = dir.resolve("file.bin");
        try {
            Files.createDirectories(dir);
            FileChannel channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            try (channel) {
                channel.write(ByteBuffer.wrap(data));
            }
            UploadStats stats = statsMap.getOrDefault(uploadId,
                    UploadStats.builder().uploadId(uploadId).chunksCount(0).bytesWritten(0L).build());
            stats.setChunksCount(chunkIndex + 1);
            stats.setBytesWritten(stats.getBytesWritten() + data.length);
            statsMap.put(uploadId, stats);
        } catch (IOException e) {
            throw new RuntimeException("Failed to append chunk", e);
        }
    }

    public void streamChunks(String uploadId, int chunkSize, ChunkConsumer consumer) {
        Path file = getUploadDir(uploadId).resolve("file.bin");
        if (!Files.exists(file)) {
            throw new IllegalArgumentException("File not found for uploadId: " + uploadId);
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            long position = 0;
            int index = 0;
            ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
            while (true) {
                buffer.clear();
                int read = channel.read(buffer, position);
                if (read <= 0) {
                    break;
                }
                buffer.flip();
                byte[] data = new byte[read];
                buffer.get(data);
                boolean isLast = (position + read) >= channel.size();
                consumer.accept(index, data, isLast);
                position += read;
                index++;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file", e);
        }
    }

    public void deleteUpload(String uploadId) {
        Path dir = getUploadDir(uploadId);
        try {
            if (Files.exists(dir)) {
                Files.walk(dir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                log.warn("Failed to delete {}", path, e);
                            }
                        });
            }
        } catch (IOException e) {
            log.warn("Failed to delete upload dir", e);
        }
        statsMap.remove(uploadId);
    }

    public UploadStats getUploadStats(String uploadId) {
        return statsMap.getOrDefault(uploadId,
                UploadStats.builder().uploadId(uploadId).chunksCount(0).bytesWritten(0L).build());
    }

    private Path getUploadDir(String uploadId) {
        return Path.of(properties.getStorage().getPath()).resolve(uploadId);
    }

    @FunctionalInterface
    public interface ChunkConsumer {
        void accept(int index, byte[] data, boolean isLast);
    }

}
