package com.example.topicadmin;

import com.example.topicadmin.controller.AdminController;
import com.example.topicadmin.dto.Dtos.TopicDetail;
import com.example.topicadmin.exception.GlobalExceptionHandler;
import com.example.topicadmin.service.ConsumerGroupService;
import com.example.topicadmin.service.DeadLetterService;
import com.example.topicadmin.service.TopicAdminService;
import com.example.topicadmin.service.TopicEnsurer;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Solo el admin entra, y los errores de validacion/Kafka se traducen a HTTP (sin cluster ni contexto de Spring). */
class AdminControllerTest {

    final TopicAdminService topics = mock(TopicAdminService.class);
    final ConsumerGroupService groups = mock(ConsumerGroupService.class);
    final DeadLetterService dlt = mock(DeadLetterService.class);
    final TopicEnsurer ensurer = mock(TopicEnsurer.class);
    final MockMvc mvc = MockMvcBuilders.standaloneSetup(new AdminController(topics, groups, dlt, ensurer))
            .setControllerAdvice(new GlobalExceptionHandler()).build();

    @Test
    void sinIdentidadEs401YNoLlegaAKafka() throws Exception {
        mvc.perform(get("/api/v1/topics")).andExpect(status().isUnauthorized());
        verifyNoInteractions(topics);
    }

    @Test
    void unUsuarioQueNoEsAdminEs403() throws Exception {
        mvc.perform(get("/api/v1/topics").header("X-User-Id", "u1").header("X-User-Role", "BUYER_SELLER"))
                .andExpect(status().isForbidden());
        mvc.perform(post("/api/v1/topics/ensure").header("X-User-Id", "u1")).andExpect(status().isForbidden());
        verifyNoInteractions(topics, ensurer);
    }

    @Test
    void elAdminVeElDetalleDeUnTopico() throws Exception {
        when(topics.get("pcb.business.events"))
                .thenReturn(new TopicDetail("pcb.business.events", Map.of("retention.ms", "604800000"), List.of()));
        mvc.perform(get("/api/v1/topics/pcb.business.events").header("X-User-Id", "a").header("X-User-Role", "WORKSHOP_ADMIN"))
                .andExpect(status().isOk()).andExpect(content().string(org.hamcrest.Matchers.containsString("604800000")));
    }

    @Test
    void elRetentionDebeSerAlMenos1000Ms() throws Exception {
        mvc.perform(patch("/api/v1/topics/pcb.business.events/retention").header("X-User-Id", "a")
                        .header("X-User-Role", "WORKSHOP_ADMIN").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"retentionMs\": 10}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(topics);
    }

    @Test
    void ensureDevuelveLosTopicosCreados() throws Exception {
        when(ensurer.ensure()).thenReturn(List.of("pcb.audit.events"));
        mvc.perform(post("/api/v1/topics/ensure").header("X-User-Id", "a").header("X-User-Role", "WORKSHOP_ADMIN"))
                .andExpect(status().isOk()).andExpect(content().json("[\"pcb.audit.events\"]"));
        verify(ensurer).ensure();
    }

    @Test
    void soloSePuedenLeerTopicosDeadLetter() throws Exception {
        when(dlt.read(eq("pcb.business.events"), any(Integer.class)))
                .thenThrow(new com.example.topicadmin.exception.ApiExceptions.BadRequestException("Solo dead-letter"));
        mvc.perform(get("/api/v1/dlt/pcb.business.events").header("X-User-Id", "a").header("X-User-Role", "WORKSHOP_ADMIN"))
                .andExpect(status().isBadRequest());
    }
}
