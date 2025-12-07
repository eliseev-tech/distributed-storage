package ru.eliseevtech.storage.coordinator.service;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DownloadInitResult {

    private String uploadId;
    private String dataNodeAddress;
    private long fileSize;

}
