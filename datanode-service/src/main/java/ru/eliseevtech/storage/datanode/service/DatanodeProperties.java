package ru.eliseevtech.storage.datanode.service;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "datanode")
public class DatanodeProperties {

    private StorageProperties storage = new StorageProperties();
    private GrpcProperties grpc = new GrpcProperties();

    @Data
    public static class StorageProperties {
        private String path;
    }

    @Data
    public static class GrpcProperties {
        private int port;
    }

}
