package com.hayden.cdcagentsdatastream.dao;

import com.google.common.collect.Lists;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.entity.CheckpointDataDiff;
import com.hayden.cdcagentsdatastream.subscriber.ctx.ContextService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface CheckpointDao {

    Logger log = LoggerFactory.getLogger(CheckpointDao.class);


    record CheckpointData(byte[] checkpoint, Timestamp checkpointNs, String threadId, String checkpointId,
                                 String taskId) {
    }

    record LatestCheckpoints(String threadId, String checkpointId, Timestamp timestamp, String taskPath) {
    }


    Map<String, List<CheckpointData>> retrieveDiffContent(CdcAgentsDataStream dataStream);

    ContextService.WithContextAdded addDiff(ContextService.WithContextAdded cdcAgentsDataStream, List<CheckpointDataDiff> diffs);

    static boolean skipParsingCheckpoint(List<CheckpointData> thisTask, Timestamp checkpointNs) {

        if (thisTask == null)
            return false;

        if (thisTask.isEmpty())
            return false;

        var thisTaskCheckpoint = thisTask.stream()
                .max(Comparator.comparing(cd -> cd.checkpointNs()))
                .orElse(null);

        byte[] checkpoint = thisTaskCheckpoint.checkpoint();

        boolean isBlank = checkpoint == null || checkpoint.length == 0;

        if (isBlank)
            return false;

        if (thisTaskCheckpoint.checkpointNs() != null)
            return thisTaskCheckpoint.checkpointNs().after(checkpointNs);

        return false;
    }

    default List<CheckpointData> validateMostRecentCheckpoint(CheckpointData checkpoint) {
        String threadId = checkpoint.threadId();
        String checkpointId = checkpoint.checkpointId();
        String task = checkpoint.taskId();
        var latest = doQueryLatestCheckpoint(threadId, task);
        return latest.stream().max(Comparator.comparing(s -> s.timestamp()))
                .filter(lc -> Objects.nonNull(lc.checkpointId()))
                .filter(s -> !Objects.equals(s.checkpointId(), checkpointId))
                .map(nF -> {
                    log.debug("""
                              Found where saving less recent checkpoint {} for thread id {}, checkpoint id {}.
                              Querying for new checkpoint blob for thread id {}, checkpoint id {}.
                              Will continue to query forever in if it keeps being updated quickly.
                              """,
                            checkpointId, threadId, nF.checkpointId(), threadId, nF.checkpointId());
                    var queries = queryCheckpointBlobForTask(threadId, nF.checkpointId(), task);
                    if (queries.isEmpty()) {
                        log.error("Error on error! Didn't find checkpoint that was found earlier!");
                        return Lists.newArrayList(checkpoint);
                    }

                    return queries;
                })
                .orElse(Lists.newArrayList(checkpoint));
    }

    List<LatestCheckpoints> doQueryLatestCheckpoint(String threadId, String taskId);

    List<CheckpointData> queryCheckpointBlobForTask(String threadId, String checkpointId, String tp);

    List<LatestCheckpoints> queryLatestCheckpoints();


    boolean doReplaceCheckpoint(CdcAgentsDataStream c, String taskId, Timestamp checkpointNs);

    boolean skipParsingCheckpoint(CdcAgentsDataStream c, String taskId, Timestamp checkpointNs);
}
