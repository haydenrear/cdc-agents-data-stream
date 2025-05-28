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

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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

    public record CheckpointData(byte[] checkpoint, Timestamp checkpointNs) {}

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
            List<CheckpointData> checkpointBlobs = jdbcTemplate.query(
                """
                SELECT timestamptz(ct.checkpoint->>'ts') as ts,
                c.blob as checkpoint_blob, ct.checkpoint AS checkpoint, c.task_id, c.idx, c.channel, c.type
                FROM checkpoint_writes c
                INNER JOIN checkpoints ct ON ct.checkpoint_id = c.checkpoint_id
                     WHERE c.thread_id = ? AND c.checkpoint_id = ?
                AND c.channel = 'messages' AND c.type = 'list'
                ORDER BY ts DESC
                LIMIT 1
                """,
                (rs, rowNum) -> {
                    return new CheckpointData(rs.getBytes("checkpoint_blob"),
                            rs.getTimestamp("ts"));
                },
                threadId, checkpointId
            );


            if (checkpointBlobs.isEmpty()) {
                log.warn("No checkpoint data found for thread_id={}, checkpoint_id={}", threadId, checkpointId);
                return Optional.empty();
            }

            return checkpointBlobs.stream()
                    .max(Comparator.comparing( cd -> cd.checkpointNs))
                    .flatMap(cd -> {
                        CdcAgentsDataStream dataStream = messageConverterService.findOrCreateDataStream(threadId);

                        // Check if this checkpoint is already stored
                        Optional<CdcAgentsDataStream> existingChunks = dataStreamRepository.findByCheckpointId(checkpointId);

                        if (existingChunks.isPresent() && existingChunks.map(c -> skipParsingCheckpoint(cd, c)).orElse(false)) {
                            // Return the already stored messages
                            return Optional.of(dataStream);
                        }

                        // Store each blob as a chunk
                        messageConverterService.convertAndSaveCheckpointData(dataStream, checkpointId, threadId, cd)
                                .peekError(err -> log.error("Failed to convert checkpoint data: {}", err.getMessage()));

                        // Return the deserialized messages
                        return Optional.of(dataStream);
                    });


        } catch (Exception e) {
            log.error("Error retrieving checkpoint: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    private static boolean skipParsingCheckpoint(CheckpointData cd, CdcAgentsDataStream c) {
        boolean isBlank = StringUtils.isBlank(c.getRawContent());
        if (isBlank)
            return false;

        return c.getCheckpointTimestamp().equals(cd.checkpointNs) || c.getCheckpointTimestamp().after(cd.checkpointNs);
    }


}