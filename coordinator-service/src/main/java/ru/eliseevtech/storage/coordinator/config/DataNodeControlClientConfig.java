package ru.eliseevtech.storage.coordinator.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.eliseevtech.storage.coordinator.client.DataNodeControlClient;

@Configuration
public class DataNodeControlClientConfig {

    @Bean
    public DataNodeControlClient dataNodeControlClient() {
        return new DataNodeControlClient();
    }

}
