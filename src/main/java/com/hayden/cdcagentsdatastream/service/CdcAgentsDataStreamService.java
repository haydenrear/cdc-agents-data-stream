package com.hayden.cdcagentsdatastream.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.model.BaseMessage;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.cdcagentsdatastream.subscriber.ctx.ContextService;
import com.hayden.utilitymodule.MapFunctions;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import com.hayden.utilitymodule.result.Result;
import com.hayden.utilitymodule.result.error.SingleError;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.hayden.cdcagentsdatastream.dao.CheckpointDao.doReplaceCheckpoint;

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
    private final CheckpointDao dao;
    private final ContextService contextService;

    @Autowired
    private DbDataSourceTrigger dbDataSourceTrigger;

    public record CdcAgentsDataStreamUpdate(
            Map<String, List<CheckpointDao.CheckpointData>> afterUpdate,
            CdcAgentsDataStream beforeUpdate) {}

    public Optional<CdcAgentsDataStream> doReadStreamItem(String threadId, String checkpointId) {
        AtomicInteger i = new AtomicInteger(-1);
        return retrieveAndStoreCheckpoint(threadId, checkpointId)
                .flatMap(sUpdate -> {
                    var startingsSeq = sUpdate.beforeUpdate.getSequenceNumber();
                    i.set(startingsSeq);
                    return contextService.addCtx(sUpdate);
                })
                .map(updatable -> {
                    CdcAgentsDataStream toSave = updatable.withCtx();
                    toSave.incrementSequenceNumber();
                    if (toSave.incrementSequenceNumber() != i.incrementAndGet()) {
                        log.error("Sequence number mismatch. Expected {} but got {}", i, toSave);
                    }
                    toSave.setRawContent(updatable.update().afterUpdate());
                    toSave.setSessionId(threadId);
                    return dataStreamRepository.save(toSave);
                });
    }

    /**
     * Converts a byte array of checkpoint data into a data stream chunk and saves it.
     *
     * @return a Result containing the saved chunk or an error
     */
    public void convertAndSaveCheckpointData(
            Map<String, List<CheckpointDao.CheckpointData>> toAddTo,
            Map<String, CheckpointDao.CheckpointData> dataEntry) {

        try {
            mergeAdd(dataEntry, toAddTo);
        } catch (Exception e) {
            log.error("Failed to convert and save checkpoint data: {}", e.getMessage());
        }
    }

    private static void mergeAdd(Map<String, CheckpointDao.CheckpointData> dataEntry,
                                 Map<String, List<CheckpointDao.CheckpointData>> toAddTo) {
        dataEntry.forEach((key, toAdd) -> {
            toAddTo.compute(key, (k,p) -> {
                if (p == null) {
                    p = new ArrayList<>();
                    p.add(toAdd);
                    return p;
                } else if (k.contains("__start__")) {
                    // we want special keeping of all previous __start__
                    if (p.stream().noneMatch(cd -> Objects.equals(cd.checkpointNs(), toAdd.checkpointNs()))) {
                        p.add(toAdd);
                    }
                    return p;
                } else {
                    p.clear();
                    p.add(toAdd);
                    return p;
                }
            });
        });
    }

    /**
     * Finds a data stream by session ID or creates a new one if it doesn't exist.
     *
     * @param sessionId the session ID to find or create a data stream for
     * @return the existing or newly created data stream
     */
    public CdcAgentsDataStream findOrCreateDataStream(String sessionId) {
        return dataStreamRepository.findBySessionId(sessionId)
                .orElseGet(() -> {
                    CdcAgentsDataStream newDataStream = new CdcAgentsDataStream();
                    newDataStream.setSessionId(sessionId);
                    return dataStreamRepository.save(newDataStream);
                });
    }

    /**
     * Retrieves checkpoint data from the checkpoint_writes table and stores it in the data stream model.
     *
     * @param threadId the thread ID of the checkpoint
     * @param checkpointId the ID of the checkpoint
     * @return an Optional containing the deserialized checkpoint data
     */
    public Optional<CdcAgentsDataStreamUpdate> retrieveAndStoreCheckpoint(String threadId, String checkpointId) {
        // Find or create the data stream
        var checkpointBlobs = dbDataSourceTrigger.doOnKey(setKey -> {
            setKey.setKey("cdc-subscriber");
            return dao.queryCheckpointBlobs(threadId, checkpointId);
        });

        if (checkpointBlobs.isEmpty()) {
            log.debug("No checkpoint data found for thread_id={}, checkpoint_id={}", threadId, checkpointId);
            return Optional.empty();
        }


        var checkpointMap = MapFunctions.CollectMap(checkpointBlobs.stream()
                .collect(Collectors.groupingBy(CheckpointDao.CheckpointData::taskId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                e -> e.stream().max(Comparator.comparing(CheckpointDao.CheckpointData::checkpointNs)))))
                .entrySet()
                .stream());


        CdcAgentsDataStream dataStream = this.findOrCreateDataStream(threadId);

        var toAddTo = dataStream.getRawContent();

        for (var e : checkpointMap.entrySet()) {

            var cdOpt = e.getValue();

            if (cdOpt.isEmpty())
                continue;

            var cd = cdOpt.get();

            if (doReplaceCheckpoint(dataStream, cd.taskId(), cd.checkpointNs())) {
                convertAndSaveCheckpointData(toAddTo, Map.of(e.getKey(), cd));
            } else {
                // Check if this checkpoint is already stored
                Optional<CdcAgentsDataStream> existingChunks = dataStreamRepository.findByCheckpointId(cd.checkpointId());

                if (existingChunks.map(c -> doReplaceCheckpoint(c, cd.taskId(), cd.checkpointNs())).orElse(true)) {
                    // Return the already stored messages
                    convertAndSaveCheckpointData(toAddTo, Map.of(e.getKey(), cd));
                }
            }
        }

        return Optional.of(new CdcAgentsDataStreamUpdate(toAddTo, dataStream));
    }

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

}