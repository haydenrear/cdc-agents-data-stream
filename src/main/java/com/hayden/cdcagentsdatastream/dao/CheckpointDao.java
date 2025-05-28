package com.hayden.cdcagentsdatastream.dao;

import com.google.common.collect.Lists;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import io.micrometer.common.util.StringUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@Component
@RequiredArgsConstructor
@Slf4j
public class CheckpointDao {

    private final JdbcTemplate jdbcTemplate;

    public record CheckpointData(byte[] checkpoint, Timestamp checkpointNs, String threadId, String checkpointId) {}

    public record LatestCheckpoints(String threadId, String checkpointId, Timestamp timestamp) {}

    public List<LatestCheckpoints> queryLatestCheckpoint(String threadId) {
        List<LatestCheckpoints> latestCheckpoints = jdbcTemplate.query(
                """
                        WITH ranked_checkpoints AS (\
                            SELECT \
                                cw.thread_id, \
                                cw.checkpoint_id, \
                                timestamptz(c.checkpoint->>'ts') as checkpoint_timestamp, \
                                ROW_NUMBER() OVER (PARTITION BY cw.thread_id ORDER BY timestamptz(c.checkpoint->>'ts') DESC) as rn \
                            FROM checkpoint_writes cw \
                            INNER JOIN checkpoints c ON c.checkpoint_id = cw.checkpoint_id \
                            WHERE cw.channel = 'messages' AND cw.type = 'list' \
                        ) \
                        SELECT thread_id, checkpoint_id, checkpoint_timestamp \
                        FROM ranked_checkpoints \
                        WHERE rn = 1
                        AND thread_id = ?
                        """,
                (rs, rowNum) -> new LatestCheckpoints(rs.getString("thread_id"),
                        rs.getString("checkpoint_id"),
                        rs.getTimestamp("checkpoint_timestamp")),
                threadId);

        return latestCheckpoints;
    }

    public List<LatestCheckpoints> queryLatestCheckpoints() {
        List<LatestCheckpoints> latestCheckpoints = jdbcTemplate.query(
                """
                        WITH ranked_checkpoints AS (\
                            SELECT \
                                cw.thread_id, \
                                cw.checkpoint_id, \
                                timestamptz(c.checkpoint->>'ts') as checkpoint_timestamp, \
                                ROW_NUMBER() OVER (PARTITION BY cw.thread_id ORDER BY timestamptz(c.checkpoint->>'ts') DESC) as rn \
                            FROM checkpoint_writes cw \
                            INNER JOIN checkpoints c ON c.checkpoint_id = cw.checkpoint_id \
                            WHERE cw.channel = 'messages' AND cw.type = 'list' \
                        ) \
                        SELECT thread_id, checkpoint_id, checkpoint_timestamp \
                        FROM ranked_checkpoints \
                        WHERE rn = 1""",
                (rs, rowNum) -> new LatestCheckpoints(rs.getString("thread_id"),
                        rs.getString("checkpoint_id"),
                        rs.getTimestamp("checkpoint_timestamp")));

        return latestCheckpoints;

    }

    public @NotNull List<CheckpointData> queryCheckpointBlobs(String threadId, String checkpointId) {
        List<CheckpointData> checkpointBlobs = jdbcTemplate.query(
            """
            SELECT\
                timestamptz(ct.checkpoint->>'ts') as ts,\
                c.blob as checkpoint_blob,\
                ct.checkpoint AS checkpoint
            FROM checkpoint_writes c
            INNER JOIN checkpoints ct\
                ON ct.checkpoint_id = c.checkpoint_id
            WHERE c.thread_id = ? AND c.checkpoint_id = ?
            AND c.channel = 'messages' AND c.type = 'list'
            ORDER BY ts DESC
            LIMIT 1
            """,
            (rs, rowNum) -> new CheckpointData(
                    rs.getBytes("checkpoint_blob"),
                    rs.getTimestamp("ts"),
                    threadId,
                    checkpointId),
                threadId, checkpointId
        );

        if (checkpointBlobs.isEmpty()) {
            return checkpointBlobs;
        }

        var cb = checkpointBlobs.getFirst();

        return validateMostRecentCheckpoint(cb);
    }

    private List<CheckpointData> validateMostRecentCheckpoint(CheckpointData checkpoint) {
        String threadId = checkpoint.threadId;
        String checkpointId = checkpoint.checkpointId;
        var latest = queryLatestCheckpoint(threadId);
        return latest.stream().max(Comparator.comparing(s -> s.timestamp))
                .filter(lc -> Objects.nonNull(lc.checkpointId))
                .filter(s -> !Objects.equals(s.checkpointId, checkpointId))
                .map(nF -> {
                    log.error("Found strange issue where saving less recent checkpoint {} for thread id {}, checkpoint id {}. Querying for new checkpoint.",
                            checkpointId, threadId, nF.checkpointId);
                    return queryCheckpointBlobs(threadId, nF.checkpointId);
                })
                .orElse(Lists.newArrayList(checkpoint));
    }

    public static boolean skipParsingCheckpoint(CheckpointData cd, CdcAgentsDataStream c) {
        boolean isBlank = StringUtils.isBlank(c.getRawContent());
        if (isBlank)
            return false;

        return c.getCheckpointTimestamp().equals(cd.checkpointNs) || c.getCheckpointTimestamp().after(cd.checkpointNs);
    }


}