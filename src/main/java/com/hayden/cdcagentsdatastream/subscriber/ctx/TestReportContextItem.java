package com.hayden.cdcagentsdatastream.subscriber.ctx;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Test report context item that contains information about test execution reports
 * related to a specific session.
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public final class TestReportContextItem implements DataStreamContextItem {

    @Column(nullable = false)
    private String sessionId;

    @Column(nullable = false)
    private LocalDateTime creationTime;
    
    private Integer sequenceNumber;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, String> testReports;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> additionalMetadata;
    
    @Override
    public Integer getSequenceNumber() {
        return sequenceNumber;
    }
}