package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;

import java.util.*;

import com.hayden.cdcagentsdatastream.entity.CheckpointDataDiff;
import com.hayden.cdcagentsdatastream.service.DataStreamService;
import com.hayden.cdcagentsdatastream.service.DiffService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;

@Service
@Slf4j
public class ContextService {

    @Autowired(required = false)
    private List<ContextProvider> contextProviders = new ArrayList<>();

    @Autowired
    private DiffService diffService;

    public record WithContextAdded(DataStreamService.CdcAgentsDataStreamUpdate update,
                                   CdcAgentsDataStream withCtx) {}

    public Optional<WithContextAdded> addCtx(DataStreamService.CdcAgentsDataStreamUpdate ds,
                                             CheckpointDao dao) {
        // Get the previous data stream for the same session (if any)
        // Get context items from all providers
        List<DataStreamContextItem> contextItems = contextProviders
            .stream()
            .flatMap(pc -> pc.retrieveCtx(ds.beforeUpdate()).stream())
            .toList();

        CdcAgentsDataStream withCtx = ds.beforeUpdate();
        var nextSequenceNumber = withCtx.getSequenceNumber() + 1;

        for (DataStreamContextItem item : contextItems) {
            item.setSequenceNumber(nextSequenceNumber);
        }

        // Use DiffService to process the diff between previous and current checkpoint data
        var updated = diffService.processDiff(ds, withCtx, dao.retrieveDiffContent(ds.beforeUpdate()));

        return Optional.of(new WithContextAdded(ds, withCtx))
                .map(wc -> dao.addDiff(wc, updated.stream().toList()));
    }
}
