package com.example.topicadmin.controller;

import com.example.topicadmin.auth.AuthContext;
import com.example.topicadmin.dto.Dtos.DltRecord;
import com.example.topicadmin.dto.Dtos.GroupLag;
import com.example.topicadmin.dto.Dtos.GroupSummary;
import com.example.topicadmin.dto.Dtos.ReplayResult;
import com.example.topicadmin.dto.Dtos.ResetOffsetsRequest;
import com.example.topicadmin.dto.Dtos.ResetResult;
import com.example.topicadmin.dto.Dtos.TopicDetail;
import com.example.topicadmin.dto.Dtos.TopicSummary;
import com.example.topicadmin.dto.Dtos.UpdateRetentionRequest;
import com.example.topicadmin.service.ConsumerGroupService;
import com.example.topicadmin.service.DeadLetterService;
import com.example.topicadmin.service.TopicAdminService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** API de administración de Kafka. Todas las rutas exigen X-User-Id y X-User-Role=WORKSHOP_ADMIN. */
@RestController
@RequestMapping("/api/v1")
public class AdminController {

    private final TopicAdminService topics;
    private final ConsumerGroupService groups;
    private final DeadLetterService dlt;

    public AdminController(TopicAdminService topics, ConsumerGroupService groups, DeadLetterService dlt) {
        this.topics = topics;
        this.groups = groups;
        this.dlt = dlt;
    }

    // ---- Tópicos ----

    @GetMapping("/topics")
    public List<TopicSummary> listTopics(@RequestHeader(value = AuthContext.USER_ID_HEADER, required = false) String userId,
                                         @RequestHeader(value = AuthContext.ROLE_HEADER, required = false) String role) {
        AuthContext.requireAdmin(userId, role);
        return topics.list();
    }

    @GetMapping("/topics/{name}")
    public TopicDetail getTopic(@RequestHeader(value = AuthContext.USER_ID_HEADER, required = false) String userId,
                                @RequestHeader(value = AuthContext.ROLE_HEADER, required = false) String role,
                                @PathVariable String name) {
        AuthContext.requireAdmin(userId, role);
        return topics.get(name);
    }

    @PatchMapping("/topics/{name}/retention")
    public TopicDetail updateRetention(@RequestHeader(value = AuthContext.USER_ID_HEADER, required = false) String userId,
                                       @RequestHeader(value = AuthContext.ROLE_HEADER, required = false) String role,
                                       @PathVariable String name,
                                       @Valid @RequestBody UpdateRetentionRequest req) {
        AuthContext.requireAdmin(userId, role);
        return topics.updateRetention(name, req);
    }

    // ---- Consumer groups ----

    @GetMapping("/groups")
    public List<GroupSummary> listGroups(@RequestHeader(value = AuthContext.USER_ID_HEADER, required = false) String userId,
                                         @RequestHeader(value = AuthContext.ROLE_HEADER, required = false) String role) {
        AuthContext.requireAdmin(userId, role);
        return groups.list();
    }

    @GetMapping("/groups/{groupId}/lag")
    public GroupLag groupLag(@RequestHeader(value = AuthContext.USER_ID_HEADER, required = false) String userId,
                             @RequestHeader(value = AuthContext.ROLE_HEADER, required = false) String role,
                             @PathVariable String groupId) {
        AuthContext.requireAdmin(userId, role);
        return groups.lag(groupId);
    }

    @PostMapping("/groups/{groupId}/reset-offsets")
    public ResetResult resetOffsets(@RequestHeader(value = AuthContext.USER_ID_HEADER, required = false) String userId,
                                    @RequestHeader(value = AuthContext.ROLE_HEADER, required = false) String role,
                                    @PathVariable String groupId,
                                    @Valid @RequestBody ResetOffsetsRequest req) {
        AuthContext.requireAdmin(userId, role);
        return groups.resetOffsets(groupId, req);
    }

    // ---- Dead-letter ----

    @GetMapping("/dlt/{topic}")
    public List<DltRecord> readDlt(@RequestHeader(value = AuthContext.USER_ID_HEADER, required = false) String userId,
                                   @RequestHeader(value = AuthContext.ROLE_HEADER, required = false) String role,
                                   @PathVariable String topic,
                                   @RequestParam(defaultValue = "50") int limit) {
        AuthContext.requireAdmin(userId, role);
        return dlt.read(topic, limit);
    }

    @PostMapping("/dlt/{topic}/replay")
    public ReplayResult replayDlt(@RequestHeader(value = AuthContext.USER_ID_HEADER, required = false) String userId,
                                  @RequestHeader(value = AuthContext.ROLE_HEADER, required = false) String role,
                                  @PathVariable String topic,
                                  @RequestParam int partition,
                                  @RequestParam long offset) {
        AuthContext.requireAdmin(userId, role);
        return dlt.replay(topic, partition, offset);
    }
}
