package ru.eliseevtech.storage.coordinator.registry;

import lombok.extern.slf4j.Slf4j;
import ru.eliseevtech.storage.coordinator.model.DataNodeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DataNodeRegistry {

    private final Map<String, DataNodeInfo> nodesById = new ConcurrentHashMap<>();
    private final AtomicInteger rrIndex = new AtomicInteger(0);
    private final long nodeTimeoutMillis;

    public DataNodeRegistry(long nodeTimeoutMillis) {
        this.nodeTimeoutMillis = nodeTimeoutMillis;
    }

    public String register(String host, int port) {
        String nodeId = UUID.randomUUID().toString();
        DataNodeInfo info = DataNodeInfo.builder()
                .nodeId(nodeId)
                .host(host)
                .port(port)
                .lastHeartbeat(System.currentTimeMillis())
                .active(true)
                .build();
        nodesById.put(nodeId, info);
        log.info("DataNode registered: {} {}:{}", nodeId, host, port);
        return nodeId;
    }

    public void unregister(String nodeId) {
        DataNodeInfo removed = nodesById.remove(nodeId);
        if (removed != null) {
            log.info("DataNode unregistered: {}", nodeId);
        }
    }

    public void heartbeat(String nodeId) {
        DataNodeInfo info = nodesById.get(nodeId);
        if (info != null) {
            info.setLastHeartbeat(System.currentTimeMillis());
            info.setActive(true);
        }
    }

    public Optional<DataNodeInfo> chooseNodeForUpload() {
        List<DataNodeInfo> activeNodes = getActiveNodes();
        if (activeNodes.isEmpty()) {
            return Optional.empty();
        }
        int idx = Math.floorMod(rrIndex.getAndIncrement(), activeNodes.size());
        return Optional.of(activeNodes.get(idx));
    }

    public List<DataNodeInfo> getActiveNodes() {
        long now = System.currentTimeMillis();
        List<DataNodeInfo> list = new ArrayList<>();
        for (DataNodeInfo info : nodesById.values()) {
            if (info.isActive() && now - info.getLastHeartbeat() < nodeTimeoutMillis) {
                list.add(info);
            }
        }
        return list;
    }

}
