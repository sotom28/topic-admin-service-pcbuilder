package com.example.topicadmin;

import com.example.topicadmin.config.TopicsProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(TopicsProperties.class)
public class TopicAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(TopicAdminApplication.class, args);
    }
}
