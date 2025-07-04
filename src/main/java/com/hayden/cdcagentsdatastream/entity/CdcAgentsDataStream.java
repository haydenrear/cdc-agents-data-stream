package com.hayden.cdcagentsdatastream.entity;

import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.subscriber.ctx.DataStreamContextItem;
import com.hayden.persistence.models.JpaHibernateAuditedIded;
import jakarta.persistence.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * Entity representing a data stream for CDC agents.
 * Each data stream is associated with a session and contains chunks of data.
 */
@Entity
@Table(name = "cdc_agents_data_stream")
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class CdcAgentsDataStream extends JpaHibernateAuditedIded {

    @Column(nullable = false, unique = true)
    private String sessionId;

    @Column(nullable = false)
    private Integer sequenceNumber = 0;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, List<CheckpointDao.CheckpointData>> cdcContent =
        new HashMap<>();

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, List<CheckpointDao.CheckpointData>> ideContent =
            new HashMap<>();

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> metadata;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<DataStreamContextItem> ctx = new ArrayList<>();

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<CheckpointDataDiff> cdcCheckpointDiffs = new ArrayList<>();

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<CheckpointDataDiff> ideCheckpointDiffs = new ArrayList<>();

    public synchronized int incrementSequenceNumber() {
        this.sequenceNumber++;
        return this.sequenceNumber;
    }
}
