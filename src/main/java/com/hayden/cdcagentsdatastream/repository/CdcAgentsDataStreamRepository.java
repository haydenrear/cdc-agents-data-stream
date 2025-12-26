package com.hayden.cdcagentsdatastream.repository;

import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface CdcAgentsDataStreamRepository extends JpaRepository<CdcAgentsDataStream, Long>, QuerydslPredicateExecutor<CdcAgentsDataStream> {

    Optional<CdcAgentsDataStream> findBySessionId(String sessionId);

    default CdcAgentsDataStream doSave(CdcAgentsDataStream dataStream) {
        return this.saveAndFlush(dataStream);
    }

    default CdcAgentsDataStream findOrCreateDataStream(String sessionId) {
        return findBySessionId(sessionId)
                .orElseGet(() -> {
                    CdcAgentsDataStream newDataStream = new CdcAgentsDataStream();
                    newDataStream.setSessionId(sessionId);
                    return doSave(newDataStream);
                });
    }

}