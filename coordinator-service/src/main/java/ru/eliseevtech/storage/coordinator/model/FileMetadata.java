package ru.eliseevtech.storage.coordinator.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class FileMetadata {

    private String filePath;
    private String uploadId;
    private String dataNodeAddress;
    private FileStatus status;
    private long fileSize;
    private long createdAt;
    private Long finalizedAt;
    private int lastChunkIndex;
    private long bytesUploaded;

}
