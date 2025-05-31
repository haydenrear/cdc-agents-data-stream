package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.LocalDateTime;

/**
 * Sealed interface representing different types of context items in the data stream.
 * Each context item is associated with a session and provides specific data.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = EnvironmentContextItem.class, name = "environment"),
        @JsonSubTypes.Type(value = TestReportContextItem.class, name = "test-report")
})
public sealed interface DataStreamContextItem permits EnvironmentContextItem, TestReportContextItem {
    /**
     * Gets the session ID this context item is associated with.
     * @return the session ID
     */
    String getSessionId();
    
    /**
     * Gets the creation timestamp of this context item.
     * @return the creation timestamp
     */
    LocalDateTime getCreationTime();
    
    /**
     * Gets the sequence number of this context item.
     * This is used to track the order of context items and their relation to checkpoint diffs.
     * @return the sequence number or null if not assigned
     */
    default Integer getSequenceNumber() {
        return null;
    }

    void setSequenceNumber(Integer sequenceNumber);
}