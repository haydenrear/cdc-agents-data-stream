package com.hayden.cdcagentsdatastream.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStreamChunk;
import com.hayden.cdcagentsdatastream.model.BaseMessage;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamChunkRepository;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.cdcagentsdatastream.service.MessageConverterService;
import com.hayden.commitdiffmodel.entity.CommitDiffContext;
import com.hayden.commitdiffmodel.repo.CdcChatTraceRepository;
import com.hayden.utilitymodule.result.Result;
import com.hayden.utilitymodule.result.error.SingleError;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class CheckpointDao {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final MessageConverterService messageConverterService;
    private final CdcAgentsDataStreamRepository dataStreamRepository;
    private final CdcAgentsDataStreamChunkRepository chunkRepository;
    private final CdcChatTraceRepository cdcChatTraceRepository;

    /**
     * Retrieves checkpoint data from the checkpoint_writes table and stores it in the data stream model.
     *
     * @param threadId the thread ID of the checkpoint
     * @param checkpointId the ID of the checkpoint
     * @return an Optional containing the deserialized checkpoint data
     */
    @Transactional
    public Optional<List<BaseMessage>> retrieveAndStoreCheckpoint(String threadId, String checkpointId) {
        try {
            // Find or create the data stream
            CdcAgentsDataStream dataStream = messageConverterService.findOrCreateDataStream(threadId);
            
            // Check if this checkpoint is already stored
            List<CdcAgentsDataStreamChunk> existingChunks = chunkRepository.findByDataStreamAndCheckpointId(dataStream, checkpointId);
            if (!existingChunks.isEmpty()) {
                // Return the already stored messages
                return messageConverterService.getAllMessages(dataStream).one().optional();
            }
            
            // Query the raw checkpoint data from the checkpoint_writes table
//             TODO: type contains NoneType, list, dict,
//                   channel contains: __start__, messages
            List<byte[]> checkpointBlobs = jdbcTemplate.query(
                """
                SELECT c.blob AS checkpoint_blob, c.task_id, c.idx, c.channel, c.type
                FROM checkpoint_writes c
                WHERE c.thread_id = ? AND c.checkpoint_id = ?
                ORDER BY c.idx ASC
                """,
                (rs, rowNum) -> {
                    return rs.getBytes("checkpoint_blob");
                },
                threadId, checkpointId
            );
            
            if (checkpointBlobs.isEmpty()) {
                log.warn("No checkpoint data found for thread_id={}, checkpoint_id={}", threadId, checkpointId);
                return Optional.empty();
            }
            
            // Store each blob as a chunk
            for (byte[] blob : checkpointBlobs) {
                messageConverterService.convertAndSaveCheckpointData(dataStream, checkpointId, blob)
                    .peekError(err -> log.error("Failed to convert checkpoint data: {}", err.getMessage()));
            }

            // Return the deserialized messages
            return messageConverterService.getAllMessages(dataStream).one().toOptional();
            
        } catch (Exception e) {
            log.error("Error retrieving checkpoint: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Retrieves all checkpoint blobs for a thread.
     *
     * @param threadId the thread ID to retrieve blobs for
     * @return a map of checkpoint IDs to their corresponding blobs
     */
    public Map<String, List<byte[]>> retrieveAllCheckpointBlobs(String threadId) {
        try {
            return jdbcTemplate.query(
                """
                SELECT c.checkpoint_id, c.blob
                FROM checkpoint_writes c
                WHERE c.thread_id = ?
                ORDER BY c.checkpoint_id, c.idx ASC
                """,
                (ResultSet rs) -> {
                    Map<String, List<byte[]>> result = new java.util.HashMap<>();
                    while (rs.next()) {
                        String checkpointId = rs.getString("checkpoint_id");
                        byte[] blob = rs.getBytes("blob");
                        result.computeIfAbsent(checkpointId, k -> new java.util.ArrayList<>()).add(blob);
                    }
                    return result;
                },
                threadId
            );
        } catch (Exception e) {
            log.error("Error retrieving all checkpoint blobs: {}", e.getMessage(), e);
            return Map.of();
        }
    }

    /**
     * Retrieves metadata for all checkpoints in a thread.
     *
     * @param threadId the thread ID to retrieve metadata for
     * @return a list of checkpoint metadata entries
     */
    public List<Map<String, Object>> retrieveCheckpointMetadata(String threadId) {
        try {
            return jdbcTemplate.query(
                """
                SELECT c.checkpoint_id, c.checkpoint_ns, c.task_id, COUNT(*) as chunk_count
                FROM checkpoint_writes c
                WHERE c.thread_id = ?
                GROUP BY c.checkpoint_id, c.checkpoint_ns, c.task_id
                ORDER BY MAX(c.idx) DESC
                """,
                (rs, rowNum) -> {
                    Map<String, Object> metadata = new java.util.HashMap<>();
                    metadata.put("checkpoint_id", rs.getString("checkpoint_id"));
                    metadata.put("checkpoint_ns", rs.getString("checkpoint_ns"));
                    metadata.put("task_id", rs.getString("task_id"));
                    metadata.put("chunk_count", rs.getInt("chunk_count"));
                    return metadata;
                },
                threadId
            );
        } catch (Exception e) {
            log.error("Error retrieving checkpoint metadata: {}", e.getMessage(), e);
            return List.of();
        }
    }

    /**
     * Retrieves all messages for a specific thread.
     *
     * @param threadId the thread ID to retrieve messages for
     * @return a Result containing the list of messages or an error
     */
    public Result<List<BaseMessage>, SingleError> retrieveAllMessagesForThread(String threadId) {
        return messageConverterService.findDataStreamByThreadId(threadId)
            .map(messageConverterService::getAllMessages)
            .orElseGet(() -> Result.err(SingleError.fromMessage("No data stream found for thread ID: " + threadId)));
    }
}