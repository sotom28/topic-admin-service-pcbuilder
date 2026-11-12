package com.example.topicadmin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

public final class Dtos {

    private Dtos() {
    }

    // ---- Tópicos ----
    public record TopicSummary(String name, int partitions, int replicationFactor, Long retentionMs,
                               String minInsyncReplicas) {
    }

    public record PartitionDto(int partition, Integer leader, List<Integer> replicas, List<Integer> isr,
                               boolean underReplicated) {
    }

    public record TopicDetail(String name, Map<String, String> config, List<PartitionDto> partitions) {
    }

    /** retentionMs: cuánto guarda Kafka los mensajes. segmentMs: cada cuánto se cierra un segmento (la retención solo borra segmentos cerrados). */
    public record UpdateRetentionRequest(@NotNull @Min(1000) Long retentionMs, @Min(1000) Long segmentMs) {
    }

    // ---- Consumer groups ----
    public record GroupSummary(String groupId, String state, int members) {
    }

    public record MemberDto(String memberId, String clientId, String host, List<String> partitions) {
    }

    public record PartitionLag(String topic, int partition, long committedOffset, long endOffset, long lag) {
    }

    public record GroupLag(String groupId, String state, long totalLag, List<MemberDto> members,
                           List<PartitionLag> partitions) {
    }

    public enum ResetMode { EARLIEST, LATEST, OFFSET }

    /** partition vacío = todas las particiones del tópico. offset solo aplica a mode=OFFSET. */
    public record ResetOffsetsRequest(@NotBlank String topic, @NotNull ResetMode mode, Long offset, Integer partition) {
    }

    public record ResetResult(String groupId, String topic, ResetMode mode, Map<Integer, Long> newOffsets) {
    }

    // ---- Dead-letter ----
    public record DltRecord(int partition, long offset, long timestamp, String key, String value,
                            Map<String, String> headers) {
    }

    public record ReplayResult(String fromTopic, String toTopic, int partition, long offset) {
    }
}
