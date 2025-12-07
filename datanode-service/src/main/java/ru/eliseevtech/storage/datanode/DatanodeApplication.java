package ru.eliseevtech.storage.datanode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import ru.eliseevtech.storage.datanode.lifecycle.CoordinatorClientProperties;
import ru.eliseevtech.storage.datanode.service.DatanodeProperties;

@SpringBootApplication
@EnableConfigurationProperties({DatanodeProperties.class, CoordinatorClientProperties.class})
public class DatanodeApplication {

    public static void main(String[] args) {
        SpringApplication.run(DatanodeApplication.class, args);
    }

}
