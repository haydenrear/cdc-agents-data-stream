package com.hayden.cdcagentsdatastream.repository;

import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CdcAgentsDataStreamRepository extends JpaRepository<CdcAgentsDataStream, Long>, QuerydslPredicateExecutor<CdcAgentsDataStream> {

    Optional<CdcAgentsDataStream> findBySessionId(String sessionId);


}