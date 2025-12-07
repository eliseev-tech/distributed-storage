package ru.eliseevtech.storage.coordinator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import ru.eliseevtech.storage.coordinator.config.CoordinatorProperties;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(CoordinatorProperties.class)
public class CoordinatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(CoordinatorApplication.class, args);
    }

}
