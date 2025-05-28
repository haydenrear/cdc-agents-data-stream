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
        @JsonSubTypes.Type(value = EnvironmentContextItem.class, name = "environment")
})
public sealed interface DataStreamContextItem permits EnvironmentContextItem {
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
    
}