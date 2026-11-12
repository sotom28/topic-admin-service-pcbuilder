package com.example.topicadmin.service;

import com.example.topicadmin.config.KafkaConfig;
import com.example.topicadmin.config.TopicsProperties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

/**
 * Asegura que existan los topicos configurados. KafkaAdmin los crea una sola vez al arrancar: si el cluster todavia no
 * estaba arriba (o se levanto despues), nunca se creaban. Esto lo reintenta cada 30 s y se puede forzar con
 * POST /api/v1/topics/ensure. Solo crea los que faltan: nunca modifica uno existente.
 */
@Service
public class TopicEnsurer {

    private static final Logger log = LoggerFactory.getLogger(TopicEnsurer.class);

    private final Admin admin;
    private final TopicsProperties props;

    public TopicEnsurer(Admin admin, TopicsProperties props) {
        this.admin = admin;
        this.props = props;
    }

    @Scheduled(initialDelayString = "${topic-admin.ensure-initial-delay-ms:10000}",
            fixedDelayString = "${topic-admin.ensure-interval-ms:30000}")
    public void scheduled() {
        try {
            List<String> created = ensure();
            if (!created.isEmpty()) {
                log.info("Topicos creados: {}", created);
            }
        } catch (RuntimeException e) {
            log.warn("No se pudo verificar los topicos (se reintenta): {}", e.getMessage());
        }
    }

    /** Crea los topicos configurados que faltan y devuelve sus nombres. */
    public List<String> ensure() {
        Set<String> existing = KafkaCalls.await(admin.listTopics().names());
        List<NewTopic> missing = props.topics().stream()
                .filter(t -> !existing.contains(t.name()))
                .map(KafkaConfig::newTopic).toList();
        if (missing.isEmpty()) {
            return List.of();
        }
        KafkaCalls.await(admin.createTopics(missing).all());
        return missing.stream().map(NewTopic::name).toList();
    }
}
