package ru.eliseevtech.storage.client;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "coordinator")
public class CoordinatorClientProperties {

    private String host;
    private int port;

}