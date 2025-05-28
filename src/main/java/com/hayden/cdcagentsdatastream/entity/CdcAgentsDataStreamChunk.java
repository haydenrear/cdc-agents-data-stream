package com.hayden.cdcagentsdatastream.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.hayden.persistence.models.JpaHibernateAuditedIded;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Entity representing a chunk of data in a CDC agents data stream.
 * Each chunk contains serialized message data and is associated with a specific data stream.
 */
@Entity
@Table(name = "cdc_agents_data_stream_chunk")
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class CdcAgentsDataStreamChunk extends JpaHibernateAuditedIded {

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "dataStream_id", nullable = false)
    @JsonIgnore
    private CdcAgentsDataStream dataStream;

    @Column(nullable = false)
    private Integer sequenceNumber;

    @Column(nullable = false)
    private String messageType;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<Map<String, Object>> messages;

    @Column(columnDefinition = "text")
    private String rawContent;

    @Column
    private String checkpointId;

    @Column
    private String taskId;

    @Column
    private Integer chunkIndex;

    @Column
    private String channel;


}