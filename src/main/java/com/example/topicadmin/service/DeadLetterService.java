package com.example.topicadmin.service;

import com.example.topicadmin.dto.Dtos.DltRecord;
import com.example.topicadmin.dto.Dtos.ReplayResult;
import com.example.topicadmin.exception.ApiExceptions.BadRequestException;
import com.example.topicadmin.exception.ApiExceptions.KafkaUnavailableException;
import com.example.topicadmin.exception.ApiExceptions.NotFoundException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/** Lee los tópicos dead-letter (*.dlt) y reenvía un mensaje al tópico original. */
@Service
public class DeadLetterService {

    private static final String DLT_SUFFIX = ".dlt";

    private final Admin admin;
    private final Map<String, Object> connection;

    public DeadLetterService(Admin admin, KafkaAdmin kafkaAdmin) {
        this.admin = admin;
        this.connection = kafkaAdmin.getConfigurationProperties();
    }

    /** Hasta `limit` mensajes por partición desde el más antiguo (con el error que dejó el consumidor en los headers). */
    public List<DltRecord> read(String dltTopic, int limit) {
        requireDlt(dltTopic);
        int max = Math.min(Math.max(limit, 1), 200);
        List<DltRecord> out = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = consumer()) {
            List<TopicPartition> parts = partitions(consumer, dltTopic);
            consumer.assign(parts);
            consumer.seekToBeginning(parts);
            Map<TopicPartition, Long> ends = consumer.endOffsets(parts);
            for (TopicPartition tp : parts) {
                long end = ends.get(tp);
                int taken = 0;
                while (consumer.position(tp) < end && taken < max) {
                    for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(2)).records(tp)) {
                        if (taken++ < max) {
                            out.add(toDto(r));
                        }
                    }
                }
            }
        }
        return out;
    }

    /** Publica de nuevo el mensaje (partición/offset del DLT) en el tópico original. No lo borra del DLT. */
    public ReplayResult replay(String dltTopic, int partition, long offset) {
        requireDlt(dltTopic);
        String target = dltTopic.substring(0, dltTopic.length() - DLT_SUFFIX.length());
        if (!KafkaCalls.await(admin.listTopics().names()).contains(target)) {
            throw new NotFoundException("El topico original no existe: " + target);
        }
        ConsumerRecord<String, String> found = null;
        try (KafkaConsumer<String, String> consumer = consumer()) {
            TopicPartition tp = new TopicPartition(dltTopic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, offset);
            for (int i = 0; i < 3 && found == null; i++) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(2)).records(tp)) {
                    if (r.offset() == offset) {
                        found = r;
                    }
                }
            }
        }
        if (found == null) {
            throw new NotFoundException("No existe el mensaje " + dltTopic + "-" + partition + "@" + offset);
        }
        try (KafkaProducer<String, String> producer = producer()) {
            producer.send(new ProducerRecord<>(target, found.key(), found.value())).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUnavailableException("Operacion interrumpida", e);
        } catch (ExecutionException e) {
            throw KafkaCalls.translate(e.getCause());
        }
        return new ReplayResult(dltTopic, target, partition, offset);
    }

    private static void requireDlt(String topic) {
        if (!topic.endsWith(DLT_SUFFIX)) {
            throw new BadRequestException("Solo se pueden leer topicos dead-letter (terminan en " + DLT_SUFFIX + ")");
        }
    }

    private List<TopicPartition> partitions(KafkaConsumer<String, String> consumer, String topic) {
        var infos = consumer.partitionsFor(topic, Duration.ofSeconds(5));
        if (infos == null || infos.isEmpty()) {
            throw new NotFoundException("El topico no existe: " + topic);
        }
        return infos.stream().map(i -> new TopicPartition(topic, i.partition())).toList();
    }

    private static DltRecord toDto(ConsumerRecord<String, String> r) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header h : r.headers()) {
            if (h.key().startsWith("kafka_dlt-") && h.value() != null) {
                String v = new String(h.value(), StandardCharsets.UTF_8);
                headers.put(h.key(), v.length() > 500 ? v.substring(0, 500) + "..." : v);
            }
        }
        return new DltRecord(r.partition(), r.offset(), r.timestamp(), r.key(), r.value(), headers);
    }

    private KafkaConsumer<String, String> consumer() {
        Map<String, Object> props = new HashMap<>(connection);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(props);
    }

    private KafkaProducer<String, String> producer() {
        Map<String, Object> props = new HashMap<>(connection);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }
}
