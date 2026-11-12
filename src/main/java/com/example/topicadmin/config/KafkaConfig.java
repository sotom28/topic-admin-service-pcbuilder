package com.example.topicadmin.config;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaConfig {

    /** KafkaAdmin crea estos tópicos al arrancar; los que ya existen no se tocan. */
    @Bean
    KafkaAdmin.NewTopics pcbTopics(TopicsProperties props) {
        NewTopic[] topics = props.topics().stream().map(KafkaConfig::newTopic).toArray(NewTopic[]::new);
        return new KafkaAdmin.NewTopics(topics);
    }

    /** Definicion Kafka de un topico configurado (la usan el arranque y el reintento de TopicEnsurer). */
    public static NewTopic newTopic(TopicsProperties.TopicDef t) {
        TopicBuilder b = TopicBuilder.name(t.name())
                .partitions(t.partitions())
                .replicas(t.replicas())
                .config("min.insync.replicas", String.valueOf(t.minInsyncReplicas()));
        if (t.retentionMs() != null) {
            b.config("retention.ms", String.valueOf(t.retentionMs()));
        }
        return b.build();
    }

    /** Cliente de administración compartido (se cierra al apagar la app). */
    @Bean(destroyMethod = "close")
    Admin adminClient(KafkaAdmin kafkaAdmin) {
        return Admin.create(kafkaAdmin.getConfigurationProperties());
    }
}
