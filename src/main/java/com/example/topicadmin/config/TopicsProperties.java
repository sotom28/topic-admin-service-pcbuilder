package com.example.topicadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.List;

/** Tópicos que se crean al arrancar (topic-admin.topics[n].* en application.properties). */
@ConfigurationProperties("topic-admin")
public record TopicsProperties(@DefaultValue List<TopicDef> topics) {

    public record TopicDef(
            String name,
            @DefaultValue("3") int partitions,
            @DefaultValue("3") int replicas,
            @DefaultValue("2") int minInsyncReplicas,
            Long retentionMs) {
    }
}
