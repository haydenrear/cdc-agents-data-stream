package com.hayden.cdcagentsdatastream.subscriber;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

@Component
@Slf4j
public class AgentPostgresService {

    @Autowired
    ObjectMapper objectMapper;

    public Optional<AgentsPostgresSubscriber.TriggerData> data(String s) {
        try {
            var created = objectMapper.readValue(s, new TypeReference<Map<String, String>>() {});
            var threadId = created.get("thread_id");
            var checkpointId = created.get("checkpoint_id");
            return Optional.of(new AgentsPostgresSubscriber.TriggerData(threadId, checkpointId));
        } catch (JsonProcessingException e) {
            log.error("Error processing checkpoint writes: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

}
