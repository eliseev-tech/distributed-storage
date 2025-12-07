package ru.eliseevtech.storage.coordinator.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataNodeInfo {

    private String nodeId;
    private String host;
    private int port;
    private long lastHeartbeat;
    private boolean active;

}
