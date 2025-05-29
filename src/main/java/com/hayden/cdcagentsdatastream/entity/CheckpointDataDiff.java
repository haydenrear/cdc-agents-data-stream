package com.hayden.cdcagentsdatastream.entity;

import com.hayden.commitdiffmodel.model.Git;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
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

    public record ContentChangeDiff(Git.ContentChange change, Timestamp timestamp) {}

    public record CheckpointDataDiffItem(List<ContentChangeDiff> changes, String taskId)  {}

    private Integer sequenceNumber;

    private Map<String, CheckpointDataDiffItem> diffData;


}