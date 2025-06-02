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
    public List<String> getSubscriptionName() {
        return List.of("ide_writes");
    }

    @Override
    public Optional<String> createSubscription() {
        @Language("sql") String toExec = """
                                        CREATE OR REPLACE FUNCTION notify_ide_trigger() RETURNS trigger AS
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
                                        EXECUTE FUNCTION notify_ide_trigger();
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
