package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import java.util.*;
import java.util.stream.Collectors;

import com.hayden.cdcagentsdatastream.service.CdcAgentsDataStreamService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ContextService {

    @Autowired(required = false)
    private List<ContextProvider> contextProviders = new ArrayList<>();

    @Autowired
    private CdcAgentsDataStreamRepository cdcAgentsDataStreamRepository;

    public record WithContextAdded(CdcAgentsDataStreamService.CdcAgentsDataStreamUpdate update,
                                   CdcAgentsDataStream withCtx) {}

    public Optional<WithContextAdded> addCtx(CdcAgentsDataStreamService.CdcAgentsDataStreamUpdate ds) {
        // Get the previous data stream for the same session (if any)
        // Get context items from all providers
        List<DataStreamContextItem> contextItems = contextProviders
            .stream()
            .flatMap(pc -> pc.retrieveCtx(ds.beforeUpdate()).stream())
            .toList();

        CdcAgentsDataStream withCtx = ds.beforeUpdate();
        var nextSequenceNumber = withCtx.getSequenceNumber() + 1;

        for (DataStreamContextItem item : contextItems) {
            if (item instanceof EnvironmentContextItem envItem) {
                envItem.setSequenceNumber(nextSequenceNumber);
            } else if (item instanceof TestReportContextItem reportItem) {
                reportItem.setSequenceNumber(nextSequenceNumber);
            }
        }

        Map<String, Object> previousCheckpoint = convertCheckpointToMap(
            withCtx.getRawContent());
        Map<String, Object> currentCheckpoint = convertCheckpointToMap(
            withCtx.getRawContent());

        withCtx.addCheckpointDiff(previousCheckpoint, currentCheckpoint);

        return Optional.of(new WithContextAdded(ds, withCtx));
    }

    /**
     * Converts checkpoint data to a simple map for diff calculation
     */
    private Map<String, Object> convertCheckpointToMap(
        Map<String, List<CheckpointDao.CheckpointData>> checkpointData
    ) {
        if (checkpointData == null) {
            return new HashMap<>();
        }

        Map<String, Object> result = new HashMap<>();
        checkpointData.forEach((key, dataList) -> {
            List<Map<String, Object>> converted = new ArrayList<>();
            for (CheckpointDao.CheckpointData data : dataList) {
                Map<String, Object> dataMap = new HashMap<>();
                converted.add(dataMap);
            }
            result.put(key, converted);
        });

        return result;
    }
}
