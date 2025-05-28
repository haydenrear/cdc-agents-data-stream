package com.hayden.cdcagentsdatastream.entity;

import com.hayden.cdcagentsdatastream.subscriber.ctx.DataStreamContextItem;
import com.hayden.persistence.models.JpaHibernateAuditedIded;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

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

    @Column(nullable = false)
    private Integer sequenceNumber = 0;

    @Column(nullable = false)
    private String messageType;

    @Column(columnDefinition = "text")
    private String rawContent;

    @Column
    private String checkpointId;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> metadata;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<DataStreamContextItem> ctx;

    @Column
    private String checkpointNamespace;

}