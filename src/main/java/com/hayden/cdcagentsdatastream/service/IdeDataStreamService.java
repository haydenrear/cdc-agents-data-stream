package com.hayden.cdcagentsdatastream.service;

import com.hayden.cdcagentsdatastream.dao.CdcCheckpointDao;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.dao.IdeCheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.lock.StripedLock;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Service for converting between JSON message data and Java objects.
 * Handles serialization and deserialization of checkpoint messages.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IdeDataStreamService {

    private final IdeCheckpointDao dao;

    private final DataStreamService dataStreamService;

    @Autowired
    private DbDataSourceTrigger dbDataSourceTrigger;


//    @Transactional
    @StripedLock
    public Optional<CdcAgentsDataStream> doReadStreamItem(String threadId, String checkpointId) {
        var checkpointData = retrieveAndStoreCheckpoint(threadId, checkpointId);
        return dataStreamService.retrieveAndStoreCheckpoint(checkpointData, threadId, dao)
                .flatMap(dataStreamService::doReadStreamItem);
    }


    /**
     * Retrieves checkpoint data from the checkpoint_writes table and stores it in the data stream model.
     *
     * @param threadId the thread ID of the checkpoint
     * @param checkpointId the ID of the checkpoint
     * @return an Optional containing the deserialized checkpoint data
     */
    public List<CheckpointDao.CheckpointData> retrieveAndStoreCheckpoint(String threadId, String checkpointId) {
        // Find or create the data stream
        return dbDataSourceTrigger.doOnKey(setKey -> {
            setKey.setKey("ide-subscriber");
            return dao.doQueryCheckpointBlobs(threadId, checkpointId);
        });

    }

}