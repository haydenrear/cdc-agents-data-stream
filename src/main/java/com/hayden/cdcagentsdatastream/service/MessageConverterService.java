package com.hayden.cdcagentsdatastream.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStreamChunk;
import com.hayden.cdcagentsdatastream.model.BaseMessage;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamChunkRepository;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.utilitymodule.result.Result;
import com.hayden.utilitymodule.result.error.SingleError;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service for converting between JSON message data and Java objects.
 * Handles serialization and deserialization of checkpoint messages.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class MessageConverterService {

    private final ObjectMapper objectMapper;
    private final CdcAgentsDataStreamRepository dataStreamRepository;
    private final CdcAgentsDataStreamChunkRepository chunkRepository;

    /**
     * Deserializes a JSON string into a list of BaseMessage objects.
     *
     * @param json the JSON string to deserialize
     * @return a Result containing the list of BaseMessage objects or an error
     */
    public Result<List<BaseMessage>, SingleError> deserializeMessages(String json) {
        try {
            List<BaseMessage> messages = objectMapper.readValue(json, new TypeReference<>() {});
            return Result.ok(messages);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize messages: {}", e.getMessage());
            return Result.err(SingleError.fromE(e, "Failed to deserialize messages"));
        }
    }

    /**
     * Deserializes a byte array into a list of BaseMessage objects.
     *
     * @param data the byte array to deserialize
     * @return a Result containing the list of BaseMessage objects or an error
     */
    public Result<List<BaseMessage>, SingleError> deserializeMessages(byte[] data) {
        return deserializeMessages(new String(data, StandardCharsets.UTF_8));
    }

    /**
     * Serializes a list of BaseMessage objects into a JSON string.
     *
     * @param messages the list of BaseMessage objects to serialize
     * @return a Result containing the JSON string or an error
     */
    public Result<String, SingleError> serializeMessages(List<BaseMessage> messages) {
        try {
            return Result.ok(objectMapper.writeValueAsString(messages));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize messages: {}", e.getMessage());
            return Result.err(SingleError.fromE(e, "Failed to serialize messages"));
        }
    }

    /**
     * Converts a list of BaseMessage objects to a map representation.
     *
     * @param messages the list of BaseMessage objects to convert
     * @return a list of maps representing the messages
     */
    public List<Map<String, Object>> convertMessagesToMaps(List<BaseMessage> messages) {
        return messages.stream()
                .map(message -> objectMapper.convertValue(message, new TypeReference<Map<String, Object>>() {}))
                .toList();
    }

    /**
     * Converts a byte array of checkpoint data into a data stream chunk and saves it.
     *
     * @param dataStream the data stream to associate the chunk with
     * @param checkpointId the ID of the checkpoint
     * @param data the checkpoint data as a byte array
     * @return a Result containing the saved chunk or an error
     */
    @Transactional
    public Result<CdcAgentsDataStreamChunk, SingleError> convertAndSaveCheckpointData(
            CdcAgentsDataStream dataStream, 
            String checkpointId, 
            byte[] data) {
        
        try {
            // Get the next sequence number
            Integer sequenceNumber = chunkRepository.findMaxSequenceNumberByDataStream(dataStream)
                    .map(num -> num + 1)
                    .orElse(0);
            
            // Deserialize the data
            String rawContent = new String(data, StandardCharsets.UTF_8);
            List<BaseMessage> messages = deserializeMessages(rawContent).one().orElseRes(new ArrayList<>());
            
            // Create and save the chunk
            CdcAgentsDataStreamChunk chunk = CdcAgentsDataStreamChunk.builder()
                    .dataStream(dataStream)
                    .sequenceNumber(sequenceNumber)
                    .messages(convertMessagesToMaps(messages))
                    .checkpointId(checkpointId)
                    .build();

            chunk.setRawContent(rawContent);
            
            dataStreamRepository.save(dataStream);
            
            return Result.ok(chunkRepository.save(chunk));
        } catch (Exception e) {
            log.error("Failed to convert and save checkpoint data: {}", e.getMessage());
            return Result.err(SingleError.fromE(e, "Failed to convert and save checkpoint data"));
        }
    }

    /**
     * Retrieves all messages from a data stream, ordered by sequence number.
     *
     * @param dataStream the data stream to retrieve messages from
     * @return a Result containing the list of all BaseMessage objects or an error
     */
    public Result<List<BaseMessage>, SingleError> getAllMessages(CdcAgentsDataStream dataStream) {
        try {
            List<CdcAgentsDataStreamChunk> chunks = chunkRepository.findByDataStreamOrderBySequenceNumberAsc(dataStream);
            List<BaseMessage> allMessages = new ArrayList<>();
            
            for (CdcAgentsDataStreamChunk chunk : chunks) {
                String rawContent = chunk.getRawContent();
                if (rawContent != null && !rawContent.isEmpty()) {
                    List<BaseMessage> messages = deserializeMessages(rawContent).one().orElseRes(new ArrayList<>());
                    allMessages.addAll(messages);
                } else if (chunk.getMessages() != null && !chunk.getMessages().isEmpty()) {
                    // Fallback to using the saved message maps
                    String json = objectMapper.writeValueAsString(chunk.getMessages());
                    List<BaseMessage> messages = deserializeMessages(json).one().orElseRes(new ArrayList<>());
                    allMessages.addAll(messages);
                }
            }
            
            return Result.ok(allMessages);
        } catch (Exception e) {
            log.error("Failed to retrieve all messages: {}", e.getMessage());
            return Result.err(SingleError.fromE(e, "Failed to retrieve all messages"));
        }
    }
    
    /**
     * Finds a data stream by session ID or creates a new one if it doesn't exist.
     *
     * @param sessionId the session ID to find or create a data stream for
     * @return the existing or newly created data stream
     */
    @Transactional
    public CdcAgentsDataStream findOrCreateDataStream(String sessionId) {
        return dataStreamRepository.findBySessionId(sessionId)
                .orElseGet(() -> {
                    CdcAgentsDataStream newDataStream = new CdcAgentsDataStream();
                    newDataStream.setSessionId(sessionId);
                    newDataStream.setThreadId(sessionId);
                    return dataStreamRepository.save(newDataStream);
                });
    }
    
    /**
     * Retrieves a data stream by thread ID.
     *
     * @param threadId the thread ID to find a data stream for
     * @return an Optional containing the data stream if found
     */
    public Optional<CdcAgentsDataStream> findDataStreamByThreadId(String threadId) {
        return dataStreamRepository.findByThreadId(threadId);
    }
}