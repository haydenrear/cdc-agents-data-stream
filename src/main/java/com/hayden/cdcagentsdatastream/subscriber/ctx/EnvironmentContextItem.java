package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.hayden.commitdiffmodel.codegen.types.Staged;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Environment context item that contains information about the git repositories,
 * commit hashes, and staged data related to a specific session.
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public final class EnvironmentContextItem implements DataStreamContextItem {

    @Column(nullable = false)
    private String sessionId;

    @Column(nullable = false)
    private LocalDateTime creationTime;
    
    private Integer sequenceNumber;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, String> gitDirectories;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, List<String>> commitHashes;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Staged stagedData;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, List<String>> codeExecutionItems;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> additionalMetadata;

}