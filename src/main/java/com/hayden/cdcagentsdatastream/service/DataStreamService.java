package com.hayden.cdcagentsdatastream.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.dao.CdcCheckpointDao;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.dao.IdeCheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.model.BaseMessage;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.cdcagentsdatastream.subscriber.ctx.ContextService;
import com.hayden.utilitymodule.MapFunctions;
import com.hayden.utilitymodule.db.WithDb;
import com.hayden.utilitymodule.result.Result;
import com.hayden.utilitymodule.result.error.SingleError;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for converting between JSON message data and Java objects.
 * Handles serialization and deserialization of checkpoint messages.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DataStreamService {

    private final ObjectMapper objectMapper;
    private final CdcAgentsDataStreamRepository dataStreamRepository;
    private final ContextService contextService;

    public record CdcAgentsDataStreamUpdate(Map<String, List<CheckpointDao.CheckpointData>> afterUpdate,
                                            CdcAgentsDataStream beforeUpdate) { }


    @Transactional
    public Optional<CdcAgentsDataStream> doReadStreamItem(CdcAgentsDataStreamUpdate sUpdate,
                                                          CheckpointDao checkpointDao) {
        return contextService.addCtx(sUpdate, checkpointDao)
                .map(this::doSave);
    }


    public CdcAgentsDataStream doSave(ContextService.WithContextAdded updatable) {
        CdcAgentsDataStream toSave = updatable.withCtx();
        toSave.setCdcContent(updatable.update().afterUpdate());
        return dataStreamRepository.saveAndFlush(toSave);
    }

    /**
     * Converts a byte array of checkpoint data into a data stream chunk and saves it.
     *
     * @return a Result containing the saved chunk or an error
     */
    public void mergeTo(
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
     * @return the existing or newly created data stream
     */

    @WithDb("cdc-data-stream")
//    @Transactional
    public Optional<CdcAgentsDataStream> doReadStreamItem(String threadId,
                                                          List<CheckpointDao.CheckpointData> checkpointData,
                                                          CdcCheckpointDao dao) {
        return retrieveAndStoreCheckpoint(checkpointData, threadId, dao)
                .flatMap(ds -> doReadStreamItem(ds, dao));
    }

    @WithDb("cdc-data-stream")
//    @Transactional
    public Optional<CdcAgentsDataStream> doReadStreamItem(String threadId,
                                                          List<CheckpointDao.CheckpointData> checkpointData,
                                                          IdeCheckpointDao dao) {
        return retrieveAndStoreCheckpoint(checkpointData, threadId, dao)
                .flatMap(ds -> doReadStreamItem(ds, dao));
    }

    /**
     * Retrieves checkpoint data from the checkpoint_writes table and stores it in the data stream model.
     *
     * @param threadId the thread ID of the checkpoint
     * @return an Optional containing the deserialized checkpoint data
     */
    private Optional<CdcAgentsDataStreamUpdate> retrieveAndStoreCheckpoint(List<CheckpointDao.CheckpointData> checkpointBlobs,
                                                                          String threadId,
                                                                          CheckpointDao checkpointData) {
        // Find or create the data stream

        if (checkpointBlobs.isEmpty()) {
            return Optional.empty();
        }

        var checkpointMap = MapFunctions.CollectMap(checkpointBlobs.stream()
                .collect(Collectors.groupingBy(CheckpointDao.CheckpointData::taskId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                e -> e.stream().max(Comparator.comparing(CheckpointDao.CheckpointData::checkpointNs)))))
                .entrySet()
                .stream());


        CdcAgentsDataStream dataStream = dataStreamRepository.findOrCreateDataStream(threadId);

        Map<String, List<CheckpointDao.CheckpointData>> toAddTo = new HashMap<>();

        for (var t : checkpointData.retrieveDiffContent(dataStream).entrySet()) {
            toAddTo.put(t.getKey(), new ArrayList<>(t.getValue()));
        }

        for (var e : checkpointMap.entrySet()) {

            var cdOpt = e.getValue();

            if (cdOpt.isEmpty())
                continue;

            var cd = cdOpt.get();

            if (checkpointData.doReplaceCheckpoint(dataStream, cd.taskId(), cd.checkpointNs())) {
                mergeTo(toAddTo, Map.of(e.getKey(), cd));
            } else {
                // Check if this checkpoint is already stored
                Optional<CdcAgentsDataStream> existingChunks = dataStreamRepository.findBySessionId(threadId);

                if (existingChunks.map(c -> checkpointData.doReplaceCheckpoint(c, cd.taskId(), cd.checkpointNs())).orElse(true)) {
                    // Return the already stored messages
                    mergeTo(toAddTo, Map.of(e.getKey(), cd));
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