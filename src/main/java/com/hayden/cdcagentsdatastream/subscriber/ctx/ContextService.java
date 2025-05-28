package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ContextService {


    @Autowired(required = false)
    private List<ContextProvider> contextProviders = new ArrayList<>();
    @Autowired
    private CdcAgentsDataStreamRepository cdcAgentsDataStreamRepository;

    public void addCtx(CdcAgentsDataStream ds) {
        var providers = contextProviders.stream()
                .flatMap(pc -> pc.retrieveCtx(ds).stream())
                .toList();

        ds.setCtx(providers);

        cdcAgentsDataStreamRepository.save(ds);
    }

}
