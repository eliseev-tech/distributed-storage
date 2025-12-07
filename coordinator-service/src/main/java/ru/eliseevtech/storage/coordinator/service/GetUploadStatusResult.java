package ru.eliseevtech.storage.coordinator.service;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GetUploadStatusResult {

    private String uploadId;
    private String status;
    private long bytesUploaded;
    private int lastChunkIndex;

}
