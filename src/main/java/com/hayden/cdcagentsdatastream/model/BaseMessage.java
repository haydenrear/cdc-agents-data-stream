package com.hayden.cdcagentsdatastream.model;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.Lists;
import com.hayden.shared.models.indicators.LongKey;
import com.hayden.shared.vector.NumpyArray;
import com.hayden.shared.vector.VectorizedNdArrayMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.checkerframework.checker.units.qual.C;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Base sealed interface for all message types in CDC agents.
 * This interface is used for Jackson deserialization of message objects
 * from the langgraph checkpoint data.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type",
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = BaseMessage.AIMessage.class, name = "ai"),
        @JsonSubTypes.Type(value = BaseMessage.HumanMessage.class, name = "human"),
        @JsonSubTypes.Type(value = BaseMessage.SystemMessage.class, name = "system"),
        @JsonSubTypes.Type(value = BaseMessage.FunctionMessage.class, name = "function"),
        @JsonSubTypes.Type(value = BaseMessage.ToolMessage.class, name = "tool")
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public sealed interface BaseMessage
        permits
            BaseMessage.AIMessage,
            BaseMessage.HumanMessage,
            BaseMessage.SystemMessage,
            BaseMessage.FunctionMessage,
            BaseMessage.ToolMessage {

    record ContentValue(List<String> content) {
        public ContentValue(String content) {
            this(Lists.newArrayList(content));
        }
    }

    class ContextValueSerializer extends StdSerializer<ContentValue> {

        protected ContextValueSerializer() {
            super(ContentValue.class);
        }

        @Override
        public void serialize(ContentValue ndArray, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeArray(ndArray.content.toArray(String[]::new), 0, ndArray.content.size());
        }
    }

    class ContentValueDeserializer extends StdDeserializer<ContentValue> {

        public ContentValueDeserializer() {
            this(null);
        }

        public ContentValueDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public ContentValue deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);

            if (node.isTextual()) {
                return new ContentValue(node.asText());
            } else if (node.isArray()) {
                ObjectMapper mapper = (ObjectMapper) jp.getCodec();
                List<String> list = mapper.convertValue(node, mapper.getTypeFactory().constructCollectionType(List.class, String.class));
                return new ContentValue(list);
            }

            throw new JsonMappingException(jp, "Invalid type for content: must be string or array of strings");
        }
    }

    /**
     * Gets the content of the message.
     *
     * @return the message content
     */
    @JsonDeserialize(using = ContentValueDeserializer.class)
    @JsonSerialize(using = ContextValueSerializer.class)
    ContentValue getContent();

    /**
     * Gets the unique identifier of the message.
     *
     * @return the message ID
     */
    UUID getId();

    /**
     * Gets the name associated with the message, if any.
     *
     * @return the name or null
     */
    String getName();

    /**
     * Gets whether this message is an example.
     *
     * @return true if this is an example message, false otherwise
     */
    boolean isExample();

    /**
     * Gets additional keyword arguments for the message.
     *
     * @return a map of additional arguments
     */
    @JsonProperty("additional_kwargs")
    Map<String, Object> getAdditionalKwargs();
    
    /**
     * Gets metadata about the response.
     *
     * @return a map of response metadata
     */
    @JsonProperty("response_metadata")
    Map<String, Object> getResponseMetadata();

    /**
     * Represents a message from an AI agent.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    final class AIMessage implements BaseMessage {
        private ContentValue content;
        private UUID id;
        private String name;
        private boolean example;
        @JsonProperty("invalid_tool_calls")
        private Map<String, Object> invalidToolCalls;
        @JsonProperty("tool_calls")
        private Map<String, Object> toolCalls;

        @JsonProperty("additional_kwargs")
        private Map<String, Object> additionalKwargs = new HashMap<>();

        @JsonProperty("response_metadata")
        private Map<String, Object> responseMetadata = new HashMap<>();

        /**
         * Creates a new AI message with the specified content.
         *
         * @param content the message content
         */
        public AIMessage(String content) {
            this.content = new ContentValue(content);
            this.id = UUID.randomUUID();
            this.example = false;
        }

        /**
         * Creates a new AI message with the specified content and name.
         *
         * @param content the message content
         * @param name the name associated with this message
         */
        public AIMessage(String content, String name) {
            this(content);
            this.name = name;
        }
    }

    /**
     * Represents a message containing function call information.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    final class FunctionMessage implements BaseMessage {
        private ContentValue content;
        private UUID id;
        private String name;
        private boolean example;

        @JsonProperty("additional_kwargs")
        private Map<String, Object> additionalKwargs = new HashMap<>();

        @JsonProperty("response_metadata")
        private Map<String, Object> responseMetadata = new HashMap<>();

        @JsonProperty("function_call")
        private Map<String, Object> functionCall;

        /**
         * Creates a new function message with the specified content.
         *
         * @param content the message content
         */
        public FunctionMessage(String content) {
            this.content = new ContentValue(content);
            this.id = UUID.randomUUID();
            this.example = false;
        }

        /**
         * Creates a new function message with the specified content and name.
         *
         * @param content the message content
         * @param name the name of the function
         */
        public FunctionMessage(String content, String name) {
            this(content);
            this.name = name;
        }

        /**
         * Creates a new function message with the specified content, name, and function call details.
         *
         * @param content the message content
         * @param name the name of the function
         * @param functionCall the function call details
         */
        public FunctionMessage(String content, String name, Map<String, Object> functionCall) {
            this(content, name);
            this.functionCall = functionCall;
        }
    }

    /**
     * Represents a message from a human user.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    final class HumanMessage implements BaseMessage {
        private ContentValue content;
        private UUID id;
        private String name;
        private boolean example;

        @JsonProperty("additional_kwargs")
        private Map<String, Object> additionalKwargs = new HashMap<>();

        @JsonProperty("response_metadata")
        private Map<String, Object> responseMetadata = new HashMap<>();

        /**
         * Creates a new human message with the specified content.
         *
         * @param content the message content
         */
        public HumanMessage(String content) {
            this.content = new ContentValue(content);
            this.id = UUID.randomUUID();
            this.example = false;
        }

        /**
         * Creates a new human message with the specified content and name.
         *
         * @param content the message content
         * @param name the name associated with this message
         */
        public HumanMessage(String content, String name) {
            this(content);
            this.name = name;
        }
    }

    /**
     * Represents a system message in the conversation.
     */
    @Data
    @NoArgsConstructor
    @Builder
    @AllArgsConstructor
    final class SystemMessage implements BaseMessage {
        private ContentValue content;
        private UUID id;
        private String name;
        private boolean example;

        @JsonProperty("additional_kwargs")
        private Map<String, Object> additionalKwargs = new HashMap<>();

        @JsonProperty("response_metadata")
        private Map<String, Object> responseMetadata = new HashMap<>();

        /**
         * Creates a new system message with the specified content.
         *
         * @param content the message content
         */
        public SystemMessage(String content) {
            this.content = new ContentValue(content);
            this.id = UUID.randomUUID();
            this.example = false;
        }

        /**
         * Creates a new system message with the specified content and name.
         *
         * @param content the message content
         * @param name the name associated with this message
         */
        public SystemMessage(String content, String name) {
            this(content);
            this.name = name;
        }
    }

    /**
     * Represents a message containing tool call information.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    final class ToolMessage implements BaseMessage {
        private ContentValue content;
        private UUID id;
        private String name;
        private boolean example;

        @JsonProperty("additional_kwargs")
        private Map<String, Object> additionalKwargs = new HashMap<>();

        @JsonProperty("response_metadata")
        private Map<String, Object> responseMetadata = new HashMap<>();

        @JsonProperty("tool_call_id")
        private String toolCallId;

        @JsonProperty("tool_name")
        private String toolName;

        /**
         * Creates a new tool message with the specified content.
         *
         * @param content the message content
         */
        public ToolMessage(String content) {
            this.content = new ContentValue(content);
            this.id = UUID.randomUUID();
            this.example = false;
        }

        /**
         * Creates a new tool message with the specified content and name.
         *
         * @param content the message content
         * @param name the name associated with this message
         */
        public ToolMessage(String content, String name) {
            this(content);
            this.name = name;
        }

        /**
         * Creates a new tool message with the specified content, tool name, and tool call ID.
         *
         * @param content the message content
         * @param toolName the name of the tool
         * @param toolCallId the ID of the tool call
         */
        public ToolMessage(String content, String toolName, String toolCallId) {
            this(content);
            this.toolName = toolName;
            this.toolCallId = toolCallId;
        }
    }
}