package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;

import java.util.Optional;

public interface ContextProvider {

    Optional<DataStreamContextItem> retrieveCtx(CdcAgentsDataStream stream);

}
