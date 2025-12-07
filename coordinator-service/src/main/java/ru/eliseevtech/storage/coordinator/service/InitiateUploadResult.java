package ru.eliseevtech.storage.coordinator.service;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class InitiateUploadResult {

    private String uploadId;
    private String dataNodeAddress;
    private int chunkSize;
    private boolean resumed;
    private int lastChunkIndex;
    private long bytesUploaded;

}
