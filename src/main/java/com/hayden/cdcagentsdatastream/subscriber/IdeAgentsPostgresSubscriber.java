package com.hayden.cdcagentsdatastream.subscriber;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.service.IdeDataStreamService;
import com.hayden.persistence.cdc.CdcSubscriber;
import lombok.extern.slf4j.Slf4j;
import org.intellij.lang.annotations.Language;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Component
public class IdeAgentsPostgresSubscriber implements AgentsPostgresSubscriber {

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private IdeDataStreamService service;
    @Autowired
    private AgentPostgresService agentPostgresService;


    @Override
    public void onDataChange(String tableName, String operation, Map<String, Object> data) {
        // Process checkpoint writes - store in our data model
        if (Objects.equals(operation, "ide_writes")) {
            var found = data.get("ide_writes");
            if (found instanceof String s) {
                try {
                    var created = objectMapper.readValue(s, new TypeReference<Map<String, String>>() {});
                    var threadId = created.get("thread_id");
                    var checkpointId = created.get("checkpoint_id");

                    if (threadId != null && checkpointId != null) {
                        // Retrieve and store the checkpoint data
                        service.doReadStreamItem(threadId, checkpointId);
                    }
                } catch (JsonProcessingException e) {
                    log.error("Error processing checkpoint writes: {}", e.getMessage(), e);
                }
            }
        }
    }
    

    @Override
    public List<String> getSubscriptionName() {
        return List.of("ide_writes");
    }

    @Override
    public Optional<String> createSubscription() {
        @Language("sql") String toExec = """
                                        CREATE OR REPLACE FUNCTION notify_trigger() RETURNS trigger AS
                                        $$
                                        BEGIN
                                            PERFORM pg_notify('ide_writes', json_build_object('thread_id', NEW.thread_id, 'checkpoint_id', NEW.checkpoint_id)::text);
                                            RETURN NEW;
                                        END;
                                        $$ LANGUAGE plpgsql;
                                        CREATE OR REPLACE TRIGGER ide_writes
                                            AFTER INSERT OR UPDATE
                                            ON ide_checkpoints
                                            FOR EACH ROW
                                        EXECUTE FUNCTION notify_trigger();
                """;
        return Optional.of(toExec);
    }

    @Override
    public Optional<CdcAgentsDataStream> doReadStreamItem(String threadId, String checkpointId) {
        return this.service.doReadStreamItem(threadId, checkpointId);
    }

    @Override
    public String name() {
        return "ide_writes";
    }

    @Override
    public Optional<TriggerData> data(String s) {
        return agentPostgresService.data(s);
    }
}
