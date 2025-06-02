package com.hayden.cdcagentsdatastream.dao;

import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.entity.CheckpointDataDiff;
import com.hayden.cdcagentsdatastream.subscriber.ctx.ContextService;
import com.hayden.cdcagentsdatastream.trigger.DbTriggerRoute;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.*;

@Component
@RequiredArgsConstructor
@Slf4j
public class CdcCheckpointDao implements CheckpointDao {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public Map<String, List<CheckpointData>> retrieveDiffContent(CdcAgentsDataStream dataStream) {
        return dataStream.getCdcContent();
    }

    @Override
    public ContextService.WithContextAdded addDiff(ContextService.WithContextAdded cdcAgentsDataStream, List<CheckpointDataDiff> diffs) {
        diffs.forEach(cdcAgentsDataStream.withCtx().getCdcCheckpointDiffs()::add);
        return cdcAgentsDataStream;
    }

    public boolean doReplaceCheckpoint(CdcAgentsDataStream c, String taskId, Timestamp checkpointNs) {
        return !skipParsingCheckpoint(c, taskId, checkpointNs);
    }

    public boolean skipParsingCheckpoint(CdcAgentsDataStream c, String taskId, Timestamp checkpointNs) {
        if (c.getCdcContent() == null || !c.getCdcContent().containsKey(taskId)) {
            return false;
        }

        var thisTask = c.getCdcContent().get(taskId);

        return CheckpointDao.skipParsingCheckpoint(thisTask, checkpointNs);
    }

    @DbTriggerRoute(route = "cdc-subscriber")
    public List<CheckpointData> doQueryCheckpointBlobs(String threadId, String checkpointId) {
        var taskPaths = queryTaskPaths(threadId, checkpointId);

        return taskPaths.stream()
                .flatMap(tp -> queryCheckpointBlobForTask(threadId, checkpointId, tp).stream())
                .toList();
    }

    @DbTriggerRoute(route = "cdc-subscriber")
    public @NotNull List<LatestCheckpoints> doQueryLatestCheckpoint(String threadId, String taskId) {
        var ck = jdbcTemplate.query(
                """
                        WITH ranked_checkpoints AS (
                            SELECT
                                cw.thread_id,
                                cw.checkpoint_id,
                                timestamptz(c.checkpoint->>'ts') as checkpoint_timestamp,
                                cw.task_path,
                                ROW_NUMBER() OVER (PARTITION BY cw.thread_id ORDER BY timestamptz(c.checkpoint->>'ts') DESC) as rn
                            FROM checkpoint_writes cw
                            INNER JOIN checkpoints c ON c.checkpoint_id = cw.checkpoint_id
                            WHERE cw.channel = 'messages' AND cw.type = 'list'
                        )
                        SELECT thread_id, checkpoint_id, checkpoint_timestamp, task_path, rn
                        FROM ranked_checkpoints
                        WHERE thread_id = ? AND task_path = ?
                        ORDER BY rn
                        LIMIT 1
                        """,
                (rs, rowNum) -> {
                    return new LatestCheckpoints(
                            rs.getString("thread_id"),
                            rs.getString("checkpoint_id"),
                            rs.getTimestamp("checkpoint_timestamp"),
                            rs.getString("task_path"));
                },
                threadId, taskId);

        return ck;
    }

    @DbTriggerRoute(route = "cdc-subscriber")
    public List<LatestCheckpoints> queryLatestCheckpoints() {
        return allTaskPaths().stream()
                .flatMap(taskPath -> {
                    var ck = jdbcTemplate.query(
                            """
                                    WITH ranked_checkpoints AS (\
                                        SELECT \
                                            cw.thread_id, \
                                            cw.checkpoint_id, \
                                            timestamptz(c.checkpoint->>'ts') as checkpoint_timestamp, \
                                            cw.task_path,\
                                            ROW_NUMBER() OVER (PARTITION BY cw.thread_id ORDER BY timestamptz(c.checkpoint->>'ts') DESC) as rn \
                                        FROM checkpoint_writes cw \
                                        INNER JOIN checkpoints c ON c.checkpoint_id = cw.checkpoint_id \
                                        WHERE cw.channel = 'messages' AND cw.type = 'list' \
                                    ) \
                                    SELECT thread_id, checkpoint_id, checkpoint_timestamp, task_path, rn \
                                    FROM ranked_checkpoints \
                                    WHERE task_path = ?
                                    ORDER BY rn
                                    LIMIT 1
                                    """,
                            (rs, rowNum) -> new LatestCheckpoints(rs.getString("thread_id"),
                                    rs.getString("checkpoint_id"),
                                    rs.getTimestamp("checkpoint_timestamp"),
                                    rs.getString("task_path")),
                            taskPath);
                    return ck.stream();
                })
                .toList();

    }

    @DbTriggerRoute(route = "cdc-subscriber")
    public @NotNull List<CheckpointData> queryCheckpointBlobForTask(String threadId, String checkpointId, String tp) {
        var checkpointBlobs = jdbcTemplate.query(
                """
                        SELECT\
                            timestamptz(ct.checkpoint->>'ts') as ts,\
                            c.blob as checkpoint_blob,\
                            ct.checkpoint AS checkpoint
                        FROM checkpoint_writes c
                        INNER JOIN checkpoints ct\
                            ON ct.checkpoint_id = c.checkpoint_id
                        WHERE c.thread_id = ? AND c.checkpoint_id = ?
                        AND c.channel = 'messages' AND c.type = 'list' AND c.task_path = ?
                        ORDER BY ts DESC
                        LIMIT 1
                        """,
                (rs, rowNum) -> new CheckpointData(rs.getBytes("checkpoint_blob"), rs.getTimestamp("ts"), threadId, checkpointId, tp),
                threadId, checkpointId, tp
        );

        if (checkpointBlobs.isEmpty()) {
            return new ArrayList<>();
        }

        var cb = checkpointBlobs.getFirst();
        return validateMostRecentCheckpoint(cb);
    }


    private @NotNull List<String> queryTaskPaths(String threadId, String checkpointId) {
        var taskPaths = jdbcTemplate.query(
                """
                        SELECT distinct t.task_path
                        FROM checkpoint_writes t
                        WHERE t.thread_id = ? AND t.checkpoint_id = ?
                        """,
                (rs, rowNum) -> rs.getString("task_path"),
                threadId, checkpointId);
        return taskPaths;
    }

    private @NotNull List<String> queryTaskPaths(String threadId) {
        var taskPaths = jdbcTemplate.query(
                """
                        SELECT distinct t.task_path
                        FROM checkpoint_writes t
                        WHERE t.thread_id = ?
                        """,
                (rs, rowNum) -> rs.getString("task_path"),
                threadId);
        return taskPaths;
    }

    private @NotNull List<String> allTaskPaths() {
        var taskPaths = jdbcTemplate.query(
                """
                        SELECT distinct t.task_path
                        FROM checkpoint_writes t
                        """,
                (rs, rowNum) -> rs.getString("task_path"));
        return taskPaths;
    }



}