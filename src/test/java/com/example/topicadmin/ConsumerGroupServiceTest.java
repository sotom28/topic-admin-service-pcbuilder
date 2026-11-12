package com.example.topicadmin;

import com.example.topicadmin.dto.Dtos.GroupLag;
import com.example.topicadmin.dto.Dtos.ResetMode;
import com.example.topicadmin.dto.Dtos.ResetOffsetsRequest;
import com.example.topicadmin.exception.ApiExceptions.ConflictException;
import com.example.topicadmin.exception.ApiExceptions.NotFoundException;
import com.example.topicadmin.service.ConsumerGroupService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Lag y offsets contra un Admin simulado (sin cluster). */
class ConsumerGroupServiceTest {

    static final TopicPartition P0 = new TopicPartition("pcb.business.events", 0);
    static final TopicPartition P1 = new TopicPartition("pcb.business.events", 1);

    final Admin admin = mock(Admin.class);
    final ConsumerGroupService service = new ConsumerGroupService(admin);

    /** Un grupo con un miembro que tiene asignadas las particiones dadas. */
    void group(String id, Set<TopicPartition> assigned, GroupState state) {
        ConsumerGroupDescription desc = mock(ConsumerGroupDescription.class);
        when(desc.groupId()).thenReturn(id);
        when(desc.groupState()).thenReturn(state);
        if (assigned.isEmpty()) {
            when(desc.members()).thenReturn(List.of());
        } else {
            MemberDescription member = mock(MemberDescription.class);
            when(member.consumerId()).thenReturn("consumer-1");
            when(member.clientId()).thenReturn("client-1");
            when(member.host()).thenReturn("/127.0.0.1");
            when(member.assignment()).thenReturn(new MemberAssignment(assigned));
            when(desc.members()).thenReturn(List.of(member));
        }
        DescribeConsumerGroupsResult described = mock(DescribeConsumerGroupsResult.class);
        when(described.all()).thenReturn(KafkaFuture.completedFuture(Map.of(id, desc)));
        when(admin.describeConsumerGroups(List.of(id))).thenReturn(described);
    }

    void committed(String id, Map<TopicPartition, OffsetAndMetadata> offsets) {
        ListConsumerGroupOffsetsResult result = mock(ListConsumerGroupOffsetsResult.class);
        when(result.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(offsets));
        when(admin.listConsumerGroupOffsets(id)).thenReturn(result);
    }

    /** latest -> el ultimo offset de cada particion; earliest -> 0. */
    void endOffsets(Map<TopicPartition, Long> latest) {
        when(admin.listOffsets(anyMap())).thenAnswer(inv -> {
            Map<TopicPartition, OffsetSpec> requested = inv.getArgument(0);
            Map<TopicPartition, ListOffsetsResultInfo> out = new HashMap<>();
            requested.forEach((tp, spec) -> {
                boolean isLatest = spec.getClass().getSimpleName().startsWith("Latest");
                out.put(tp, new ListOffsetsResultInfo(isLatest ? latest.get(tp) : 0L, 0L, Optional.empty()));
            });
            ListOffsetsResult result = mock(ListOffsetsResult.class);
            when(result.all()).thenReturn(KafkaFuture.completedFuture(out));
            return result;
        });
    }

    @Test
    void elLagEsElUltimoOffsetMenosElConfirmadoPorParticion() {
        group("analytics-aggregator", Set.of(P0, P1), GroupState.STABLE);
        committed("analytics-aggregator", Map.of(P0, new OffsetAndMetadata(4), P1, new OffsetAndMetadata(10)));
        endOffsets(Map.of(P0, 10L, P1, 10L));

        GroupLag lag = service.lag("analytics-aggregator");
        assertEquals(6, lag.totalLag());
        assertEquals(2, lag.partitions().size());
        assertEquals(6, lag.partitions().get(0).lag());
        assertEquals(0, lag.partitions().get(1).lag());
    }

    @Test
    void unaParticionAsignadaSinOffsetConfirmadoCuentaComoLagDesdeElInicio() {
        // grupo nuevo: P0 ya confirmo, P1 todavia no leyo nada (antes aparecia con lag 0 o no aparecia)
        group("audit-writer", Set.of(P0, P1), GroupState.STABLE);
        Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
        committed.put(P0, new OffsetAndMetadata(2));
        committed("audit-writer", committed);
        endOffsets(Map.of(P0, 2L, P1, 5L));

        GroupLag lag = service.lag("audit-writer");
        assertEquals(5, lag.totalLag());
        var p1 = lag.partitions().stream().filter(p -> p.partition() == 1).findFirst().orElseThrow();
        assertEquals(0, p1.committedOffset());
        assertEquals(5, p1.lag());
    }

    @Test
    void unGrupoSinParticionesNiOffsetsTieneLagCero() {
        group("vacio", Set.of(), GroupState.EMPTY);
        committed("vacio", Map.of());

        GroupLag lag = service.lag("vacio");
        assertEquals(0, lag.totalLag());
        assertTrue(lag.partitions().isEmpty());
    }

    @Test
    void noSePuedenResetearOffsetsDeUnGrupoConConsumidoresActivos() {
        group("analytics-aggregator", Set.of(P0), GroupState.STABLE);
        assertThrows(ConflictException.class, () -> service.resetOffsets("analytics-aggregator",
                new ResetOffsetsRequest("pcb.business.events", ResetMode.EARLIEST, null, null)));
    }

    @Test
    void unGrupoInexistenteEsUn404() {
        group("fantasma", Set.of(), GroupState.DEAD);
        assertThrows(NotFoundException.class, () -> service.lag("fantasma"));
    }
}
