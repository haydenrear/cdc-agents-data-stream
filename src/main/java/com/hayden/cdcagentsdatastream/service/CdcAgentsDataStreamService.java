package com.hayden.cdcagentsdatastream.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.model.BaseMessage;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.utilitymodule.result.Result;
import com.hayden.utilitymodule.result.error.SingleError;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Service for converting between JSON message data and Java objects.
 * Handles serialization and deserialization of checkpoint messages.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CdcAgentsDataStreamService {

    private final ObjectMapper objectMapper;
    private final CdcAgentsDataStreamRepository dataStreamRepository;

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
     * Converts a byte array of checkpoint data into a data stream chunk and saves it.
     *
     * @param dataStream the data stream to associate the chunk with
     * @param checkpointId the ID of the checkpoint
     * @param data the checkpoint data as a byte array
     * @return a Result containing the saved chunk or an error
     */
    @Transactional
    public Result<CdcAgentsDataStream, SingleError> convertAndSaveCheckpointData(
            CdcAgentsDataStream dataStream, 
            String checkpointId,
            String threadId,
            byte[] data) {
        
        try {
            // Get the next sequence number
            // Deserialize the data
            String rawContent = new String(data, StandardCharsets.UTF_8);

            List<BaseMessage> messages = deserializeMessages(rawContent).one().orElseRes(new ArrayList<>());

            if (dataStream.getRawContent() == null || messages.stream().allMatch(bm -> Objects.nonNull(bm.getName()))) {
                // Create and save the chunk
                dataStream.setCheckpointId(checkpointId);
                dataStream.setSessionId(threadId);
                dataStream.setSequenceNumber(dataStream.getSequenceNumber() + 1);
                dataStream.setRawContent(rawContent);

                return Result.ok(dataStreamRepository.save(dataStream));
            }

            return Result.empty();

        } catch (Exception e) {
            log.error("Failed to convert and save checkpoint data: {}", e.getMessage());
            return Result.err(SingleError.fromE(e, "Failed to convert and save checkpoint data"));
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
                    return dataStreamRepository.save(newDataStream);
                });
    }

}