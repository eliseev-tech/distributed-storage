package ru.eliseevtech.storage.datanode.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UploadStats {

    private String uploadId;
    private int chunksCount;
    private long bytesWritten;

}
