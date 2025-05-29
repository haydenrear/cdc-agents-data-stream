package com.hayden.cdcagentsdatastream.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Represents a diff between consecutive CheckpointData objects.
 * This allows storing only the changes between checkpoints rather than duplicating the entire data.
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class CheckpointDataDiff {

    private Long id;

    private Integer sequenceNumber;

    private Map<String, Object> diffData;

    public static Map<String, Object> calculateDiff(Map<String, Object> previous, Map<String, Object> current) {
        // Implementation would compare the two maps and return only the differences
        // This is a placeholder for the actual diff calculation logic
        return current;
    }
}