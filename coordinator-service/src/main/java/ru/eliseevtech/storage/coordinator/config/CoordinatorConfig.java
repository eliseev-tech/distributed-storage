package ru.eliseevtech.storage.coordinator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.eliseevtech.storage.coordinator.registry.DataNodeRegistry;
import ru.eliseevtech.storage.coordinator.storage.JsonFileMetadataStore;

import java.nio.file.Path;

@Configuration
public class CoordinatorConfig {

    @Bean
    public JsonFileMetadataStore metadataStore(CoordinatorProperties props, ObjectMapper mapper) {
        Path path = Path.of(props.getStorage().getPath());
        return new JsonFileMetadataStore(path, mapper);
    }

    @Bean
    public DataNodeRegistry dataNodeRegistry(CoordinatorProperties props) {
        long timeout = props.getCleanup().getTimeoutMs();
        return new DataNodeRegistry(timeout);
    }

}
