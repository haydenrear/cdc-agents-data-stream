package com.hayden.cdcagentsdatastream.subscriber;

import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.service.CdcAgentsDataStreamService;
import lombok.extern.slf4j.Slf4j;
import org.intellij.lang.annotations.Language;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class CdcAgentsPostgresSubscriber implements AgentsPostgresSubscriber {

    @Autowired
    private CdcAgentsDataStreamService cdcAgentsDataStreamService;
    @Autowired
    private AgentPostgresService agentPostgresService;


    @Override
    public List<String> getSubscriptionName() {
        return List.of("cdc_checkpoint_writes");
    }

    @Override
    public Optional<String> createSubscription() {
        @Language("sql") String toExec = """
                                        CREATE OR REPLACE FUNCTION notify_cdc_trigger() RETURNS trigger AS
                                        $$
                                        BEGIN
                                            PERFORM pg_notify('cdc_checkpoint_writes', json_build_object('thread_id', NEW.thread_id, 'checkpoint_id', NEW.checkpoint_id)::text);
                                            RETURN NEW;
                                        END;
                                        $$ LANGUAGE plpgsql;
                                        CREATE OR REPLACE TRIGGER cdc_checkpoint_writes
                                            AFTER INSERT OR UPDATE
                                            ON checkpoint_writes
                                            FOR EACH ROW
                                        EXECUTE FUNCTION notify_cdc_trigger();
                """;
        return Optional.of(toExec);
    }

    @Override
    public Optional<CdcAgentsDataStream> doReadStreamItem(String threadId, String checkpointId) {
        return cdcAgentsDataStreamService.doReadStreamItem(threadId, checkpointId);
    }

    @Override
    public String name() {
        return "cdc_checkpoint_writes";
    }

    @Override
    public Optional<AgentsPostgresSubscriber.TriggerData> data(String s) {
        return agentPostgresService.data(s);
    }
}
