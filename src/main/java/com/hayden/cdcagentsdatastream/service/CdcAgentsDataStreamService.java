package com.hayden.cdcagentsdatastream.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.model.BaseMessage;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.utilitymodule.result.Result;
import com.hayden.utilitymodule.result.error.SingleError;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
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

    public Result<LocalDateTime, SingleError> retrieveTimestamp(byte[] json) {
        try {
            Map<String, Object> messages = objectMapper.readValue(json, new TypeReference<>() {});
            return Result.ok(LocalDateTime.parse(String.valueOf(messages.get("ts"))));
        } catch (IOException |
                 DateTimeParseException e) {
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
            CheckpointDao.CheckpointData dataEntry) {
        
        try {


            var data = dataEntry.checkpoint();
            String rawContent = new String(data, StandardCharsets.UTF_8);


            dataStream.setCheckpointId(checkpointId);
            dataStream.setSessionId(threadId);
            dataStream.setSequenceNumber(dataStream.getSequenceNumber() + 1);
            dataStream.setRawContent(rawContent);
            dataStream.setCheckpointTimestamp(dataEntry.checkpointNs());

            return Result.ok(dataStreamRepository.save(dataStream));

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