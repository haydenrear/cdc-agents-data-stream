package com.hayden.cdcagentsdatastream.subscriber;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Striped;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.service.CdcAgentsDataStreamService;
import com.hayden.cdcagentsdatastream.subscriber.ctx.ContextService;
import com.hayden.persistence.cdc.CdcSubscriber;
import lombok.extern.slf4j.Slf4j;
import org.intellij.lang.annotations.Language;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Component
public class CdcAgentsPostgresSubscriber implements CdcSubscriber {

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private CdcAgentsDataStreamService service;


    Striped<Lock> locks = Striped.lock(1024);

    @Override
    public void onDataChange(String tableName, String operation, Map<String, Object> data) {
        // Process checkpoint writes - store in our data model
        if (Objects.equals(operation, "cdc_checkpoint_writes")) {
            var found = data.get("cdc_checkpoint_writes");
            if (found instanceof String s) {
                try {
                    var created = objectMapper.readValue(s, new TypeReference<Map<String, String>>() {});
                    var threadId = created.get("thread_id");
                    var checkpointId = created.get("checkpoint_id");

                    if (threadId != null && checkpointId != null) {
                        // Retrieve and store the checkpoint data
                        Lock threadLock = locks.get(threadId);
                        threadLock.lock();
                        try {
                            service.doReadStreamItem(threadId, checkpointId);
                        } finally {
                            threadLock.unlock();
                        }
                    }
                } catch (JsonProcessingException e) {
                    log.error("Error processing checkpoint writes: {}", e.getMessage(), e);
                }
            }
        }
    }
    

    @Override
    public List<String> getSubscriptionName() {
        return List.of("cdc_checkpoint_writes");
    }

    @Override
    public Optional<String> createSubscription() {
        @Language("sql") String toExec = """
                                        CREATE OR REPLACE FUNCTION notify_trigger() RETURNS trigger AS
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
                                        EXECUTE FUNCTION notify_trigger();
                """;
        return Optional.of(toExec);
    }
}
