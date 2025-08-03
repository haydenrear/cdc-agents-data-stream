package com.hayden.cdcagentsdatastream.entity;

import com.hayden.commitdiffcontext.model.Git;
import lombok.*;

import java.sql.Timestamp;
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

    public record ContentChangeDiff(Git.Content change, Timestamp timestamp) {}

    public record CheckpointDataDiffItem(List<ContentChangeDiff> changes, String taskId)  {}

    private Integer sequenceNumber;

    private Map<String, CheckpointDataDiffItem> diffData;


}