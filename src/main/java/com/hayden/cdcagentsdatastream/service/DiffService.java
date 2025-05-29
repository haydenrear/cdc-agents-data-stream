package com.hayden.cdcagentsdatastream.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.difflib.DiffUtils;
import com.github.difflib.patch.*;
import com.google.common.collect.Sets;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.entity.CheckpointDataDiff;

import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hayden.commitdiffmodel.model.Git;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.Diff;
import org.apache.commons.lang3.builder.DiffBuilder;
import org.apache.commons.lang3.builder.DiffResult;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.text.diff.CommandVisitor;
import org.apache.commons.text.diff.StringsComparator;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;

/**
 * Service for handling the calculation and storage of diffs between checkpoints.
 * Extracts the diff logic from ContextService to follow single responsibility principle.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DiffService {

    /**
     * Processes a data stream update and calculates the diff between previous and current checkpoint data.
     * Adds the calculated diff to the data stream's checkpointDiffs list.
     *
     * @param update the data stream update containing before and after states
     * @return the updated CdcAgentsDataStream with the new diff added
     */
    public CdcAgentsDataStream processDiff(CdcAgentsDataStreamService.CdcAgentsDataStreamUpdate update,
                                           CdcAgentsDataStream dataStream) {

        var c = getCheckpointDataDiff(dataStream, dataStream.getRawContent(), update.afterUpdate());

        dataStream.getCheckpointDiffs().add(c);

        return dataStream;
    }

    /**
     * Adds a new checkpoint diff to the data stream
     *
     * @param dataStream         the data stream to add the diff to
     * @param previousCheckpoint the previous checkpoint data
     * @param currentCheckpoint  the current checkpoint data
     * @return
     */
    private CheckpointDataDiff getCheckpointDataDiff(CdcAgentsDataStream dataStream,
                                                     Map<String, List<CheckpointDao.CheckpointData>> previousCheckpoint,
                                                     Map<String, List<CheckpointDao.CheckpointData>> currentCheckpoint) {
        int seq = dataStream.getSequenceNumber() + 1;
        var diffData = calculateDiff(previousCheckpoint, currentCheckpoint, seq);
        return diffData;
    }

    /**
     * Calculate the differences between previous and current checkpoint data
     *
     * @param previous the previous checkpoint data
     * @param current  the current checkpoint data
     * @return a map containing only the differences between the two maps
     */
    private CheckpointDataDiff calculateDiff(Map<String, List<CheckpointDao.CheckpointData>> previous,
                                             Map<String, List<CheckpointDao.CheckpointData>> current,
                                             int seq) {
        Map<String, CheckpointDataDiff.CheckpointDataDiffItem> diff = new HashMap<>();

        var allKeys = Sets.union(previous.keySet(), current.keySet());

        for (String key : allKeys) {
            if (!previous.containsKey(key)) {
                diff.put(key, createDiffAdd(current.get(key), key));
            } else if (!current.containsKey(key)) {
                diff.put(key, createDiffRemove(previous.get(key), key));
            } else {
                List<CheckpointDao.CheckpointData> prevValue = previous.get(key);
                List<CheckpointDao.CheckpointData> currValue = current.get(key);

                Function<List<CheckpointDao.CheckpointData>, List<ParsedDiff>> downstream = s -> s.stream()
                        .map(cd -> new ParsedDiff(new String(cd.checkpoint(), Charset.defaultCharset()), cd))
                        .toList();

                var prevParsed = parse(prevValue, downstream);
                var currParsed = parse(currValue, downstream);

                if (prevParsed.size() > 1 || currParsed.size() > 1) {
                    log.error("Found multiple keys for checkpoint data! Failed.");
                }
                if (prevParsed.size() == 1 && !prevParsed.containsKey(key)) {
                    log.error("Found multiple keys for checkpoint data! Failed.");
                }
                if (currParsed.size() == 1 && !currParsed.containsKey(key)) {
                    log.error("Found multiple keys for checkpoint data! Failed.");
                }

                var p = prevParsed.get(key);
                var c = currParsed.get(key);

                var computedDiff = computeDiff(p, c, key);
                diff.put(key, computedDiff);
            }
        }

        return new CheckpointDataDiff(seq, diff);

    }

    public record ParsedDiff(String checkpoint, CheckpointDao.CheckpointData data) {}

    private static Map<String, List<ParsedDiff>> parse(List<CheckpointDao.CheckpointData> prevValue,
                                                       Function<List<CheckpointDao.CheckpointData>, List<ParsedDiff>> downstream) {
        return prevValue.stream()
                .sorted(Comparator.comparing(CheckpointDao.CheckpointData::checkpointNs))
                .collect(
                        Collectors.groupingBy(
                                CheckpointDao.CheckpointData::taskId,
                                Collectors.collectingAndThen(Collectors.toList(), downstream)));
    }

    /**
     * Creates a diff entry with operation type and values
     */
    private CheckpointDataDiff.CheckpointDataDiffItem createDiffAdd(List<CheckpointDao.CheckpointData> newValue, String taskId) {
        return createDiffFromFactory(newValue, c -> {
            var s  = new String(c.checkpoint(), Charset.defaultCharset());
            Git.ContentChange co = new Git.ContentChange.InsertContent(new Git.ContentChange.DiffRangeItem(0, s.length()), Arrays.asList(s.split(System.lineSeparator())));
            return co;
        }, taskId);
    }

    private CheckpointDataDiff.CheckpointDataDiffItem createDiffFromFactory(List<CheckpointDao.CheckpointData> newValue,
                                                                            Function<CheckpointDao.CheckpointData, Git.ContentChange> factory,
                                                                            String taskId) {
        var f = newValue.stream()
                .map(cd -> {
                    Git.ContentChange c = factory.apply(cd);
                    return new CheckpointDataDiff.ContentChangeDiff(c, cd.checkpointNs());
                })
                .toList();
        return new CheckpointDataDiff.CheckpointDataDiffItem(f, taskId);
    }

    /**
     * Creates a diff entry with operation type and values
     */
    private CheckpointDataDiff.CheckpointDataDiffItem createDiffRemove(List<CheckpointDao.CheckpointData> oldValue, String taskId) {
        return createDiffFromFactory(oldValue, c -> {
            var s  = new String(c.checkpoint(), Charset.defaultCharset());
            Git.ContentChange co = new Git.ContentChange.RemoveContent(new Git.ContentChange.DiffRangeItem(0, s.length()), Arrays.asList(s.split(System.lineSeparator())));
            return co;
        }, taskId);
    }

    /**
     * Computes string difference using Apache Commons Text
     */
    private CheckpointDataDiff.CheckpointDataDiffItem computeDiff(List<ParsedDiff> left, List<ParsedDiff> right,
                                                                  String taskId) {
        var foundLeft = parseTo(left);
        var foundRight = parseTo(right);

        var maxTimestamp = Stream.concat(left.stream(), right.stream())
                .map(pd -> pd.data.checkpointNs())
                .max(Comparator.comparing(s -> s))
                .orElseGet(() -> {
                    log.error("Error - did not have any timestamp!");
                    return null;
                });

        var diffed = DiffUtils.diffInline(foundLeft, foundRight);
        var c = diffed.getDeltas().stream()
                .map(delta -> switch(delta) {
                    case EqualDelta ignored -> null;
                    case ChangeDelta cd -> {
                        var iStr = (ChangeDelta<String>) cd;
                        var found = iStr.getTarget();
                        var foundSrc = iStr.getSource();
                        yield new CheckpointDataDiff.ContentChangeDiff(
                                new Git.ContentChange.ReplaceContent(buildRemove(found),buildInsert(foundSrc) ), maxTimestamp);
                    }
                    case DeleteDelta r -> {
                        var iStr = (DeleteDelta<String>) r;
                        var found = iStr.getSource();
                        Git.ContentChange.RemoveContent remove = buildRemove(found);
                        yield new CheckpointDataDiff.ContentChangeDiff(remove, maxTimestamp);

                    }
                    case InsertDelta i -> {
                        var iStr = (InsertDelta<String>) i;
                        var found = iStr.getTarget();
                        yield new CheckpointDataDiff.ContentChangeDiff(buildInsert(found), maxTimestamp);
                    }
                    default -> {
                        log.error("Found unknown delta.");
                        yield null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();

        return new CheckpointDataDiff.CheckpointDataDiffItem(c, taskId);
    }

    private static Git.ContentChange.@NotNull RemoveContent buildRemove(Chunk<String> found) {
        List<Integer> changePosition = found.getChangePosition();
        var first = changePosition.getFirst();
        var last = changePosition.getLast();
        Git.ContentChange.RemoveContent remove = new Git.ContentChange.RemoveContent(new Git.ContentChange.DiffRangeItem(first, last), found.getLines());
        return remove;
    }

    private static Git.ContentChange.@NotNull InsertContent buildInsert(Chunk<String> found) {
        List<Integer> changePosition = found.getChangePosition();
        var first = changePosition.getFirst();
        var last = changePosition.getLast();
        var remove = new Git.ContentChange.InsertContent(new Git.ContentChange.DiffRangeItem(first, last), found.getLines());
        return remove;
    }

    private static @NotNull String parseTo(List<ParsedDiff> right) {
        var foundRight  = right.stream().sorted(Comparator.comparing(s -> s.data.checkpointNs()))
                .map(pd -> pd.checkpoint)
                .collect(Collectors.joining());
        return foundRight;
    }

    /**
     * Check if two objects are equal, handling nulls
     */
    private boolean objectsEqual(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }
}
