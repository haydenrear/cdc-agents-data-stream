package com.hayden.cdcagentsdatastream.repository;

import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStreamChunk;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CdcAgentsDataStreamChunkRepository extends JpaRepository<CdcAgentsDataStreamChunk, Long>, QuerydslPredicateExecutor<CdcAgentsDataStreamChunk> {
    List<CdcAgentsDataStreamChunk> findByDataStream(CdcAgentsDataStream dataStream);
    
    List<CdcAgentsDataStreamChunk> findByDataStreamOrderBySequenceNumberAsc(CdcAgentsDataStream dataStream);
    
    Optional<CdcAgentsDataStreamChunk> findByDataStreamAndSequenceNumber(CdcAgentsDataStream dataStream, Integer sequenceNumber);
    
    @Query("SELECT MAX(c.sequenceNumber) FROM CdcAgentsDataStreamChunk c WHERE c.dataStream = :dataStream")
    Optional<Integer> findMaxSequenceNumberByDataStream(@Param("dataStream") CdcAgentsDataStream dataStream);
    
    List<CdcAgentsDataStreamChunk> findByDataStreamAndCheckpointId(CdcAgentsDataStream dataStream, String checkpointId);
    
    void deleteByDataStream(CdcAgentsDataStream dataStream);
}