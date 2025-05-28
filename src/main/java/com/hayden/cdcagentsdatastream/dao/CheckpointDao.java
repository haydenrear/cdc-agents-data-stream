package com.hayden.cdcagentsdatastream.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.cdcagentsdatastream.service.CdcAgentsDataStreamService;
import com.hayden.commitdiffmodel.repo.CdcChatTraceRepository;
import io.micrometer.common.util.StringUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class CheckpointDao {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final CdcAgentsDataStreamService messageConverterService;
    private final CdcAgentsDataStreamRepository dataStreamRepository;
    private final CdcChatTraceRepository cdcChatTraceRepository;

    /**
     * Retrieves checkpoint data from the checkpoint_writes table and stores it in the data stream model.
     *
     * @param threadId the thread ID of the checkpoint
     * @param checkpointId the ID of the checkpoint
     * @return an Optional containing the deserialized checkpoint data
     */
    @Transactional
    public Optional<CdcAgentsDataStream> retrieveAndStoreCheckpoint(String threadId, String checkpointId) {
        try {
            // Find or create the data stream
            CdcAgentsDataStream dataStream = messageConverterService.findOrCreateDataStream(threadId);
            
            // Check if this checkpoint is already stored
            Optional<CdcAgentsDataStream> existingChunks = dataStreamRepository.findByCheckpointId(checkpointId);

            if (existingChunks.isPresent() && existingChunks.map(c -> StringUtils.isNotBlank(c.getRawContent())).orElse(false)) {
                // Return the already stored messages
                return Optional.of(dataStream);
            }
            
            List<byte[]> checkpointBlobs = jdbcTemplate.query(
                """
                SELECT c.blob AS checkpoint_blob, c.task_id, c.idx, c.channel, c.type
                FROM checkpoint_writes c
                WHERE c.thread_id = ? AND c.checkpoint_id = ?
                AND c.channel = 'messages' AND c.type = 'list'
                ORDER BY LENGTH(checkpoint_blob) DESC
                LIMIT 1
                """,
                (rs, rowNum) -> rs.getBytes("checkpoint_blob"),
                threadId, checkpointId
            );
            
            if (checkpointBlobs.isEmpty()) {
                log.warn("No checkpoint data found for thread_id={}, checkpoint_id={}", threadId, checkpointId);
                return Optional.empty();
            }

            if (checkpointBlobs.size() > 1) {
                log.error("Found extra checkpoint blobs. Not sure what do do with the extras?");
            }
            
            // Store each blob as a chunk
            for (byte[] blob : checkpointBlobs) {
                messageConverterService.convertAndSaveCheckpointData(dataStream, checkpointId, threadId, blob)
                    .peekError(err -> log.error("Failed to convert checkpoint data: {}", err.getMessage()));
            }

            // Return the deserialized messages
            return Optional.of(dataStream);
            
        } catch (Exception e) {
            log.error("Error retrieving checkpoint: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }


}