package com.hayden.cdcagentsdatastream.entity;

import com.hayden.commitdiffmodel.entity.CommitDiffContext;
import com.hayden.persistence.models.JpaHibernateAuditedIded;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Entity representing a data stream for CDC agents.
 * Each data stream is associated with a session and contains chunks of data.
 */
@Entity
@Table(name = "cdc_agents_data_stream", indexes = {
        @Index(name = "idx_cdc_agents_data_stream_session_id", columnList = "sessionId", unique = true)
})
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class CdcAgentsDataStream extends JpaHibernateAuditedIded {

    @Column(nullable = false, unique = true)
    private String sessionId;

    @OneToMany(mappedBy = "dataStream", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<CdcAgentsDataStreamChunk> chunks = new ArrayList<>();

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> metadata;

    @Column
    private String checkpointNamespace;

    @Column
    private String threadId;


    /**
     * Adds a chunk to this data stream.
     *
     * @param chunk the chunk to add
     */
    public void addChunk(CdcAgentsDataStreamChunk chunk) {
        chunks.add(chunk);
        chunk.setDataStream(this);
    }
}