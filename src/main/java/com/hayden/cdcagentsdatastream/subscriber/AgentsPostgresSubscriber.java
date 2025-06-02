package com.hayden.cdcagentsdatastream.subscriber;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.persistence.cdc.CdcSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public interface AgentsPostgresSubscriber extends CdcSubscriber {

    Logger log = LoggerFactory.getLogger(AgentsPostgresSubscriber.class);

    record TriggerData(String threadId, String checkpointId) {
    }


    Optional<CdcAgentsDataStream> doReadStreamItem(String threadId, String checkpointId);

    String name();

    Optional<TriggerData> data(String s);

    default void onDataChange(String tableName, String operation, Map<String, Object> data) {
        // Process checkpoint writes - store in our data model
        if (Objects.equals(operation, name())) {
            var found = data.get(name());
            if (found instanceof String s) {
                var created = data(s);

                if (created.isEmpty())
                    throw new RuntimeException("Was not able to read %s.".formatted(s));

                var createdTriggerData = created.get();

                var threadId = createdTriggerData.threadId;
                var checkpointId = createdTriggerData.checkpointId;

                if (threadId != null && checkpointId != null) {
                    // Retrieve and store the checkpoint data
                    doReadStreamItem(threadId, checkpointId);
                }
            }
        }
    }

}
