package com.hayden.cdcagentsdatastream.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.dao.CdcCheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.entity.CheckpointDataDiff;
import com.hayden.commitdiffmodel.model.Git;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@ExtendWith(MockitoExtension.class)
class DiffServiceTest {

    @InjectMocks
    private DiffService diffService;
    
    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @ParameterizedTest
    @ValueSource(strings = {
//        "diffservice/case1_addition",
//        "diffservice/case2_removal",
//        "diffservice/case3_modification",
        "diffservice/case4_multiple_changes"
    })
    void testDiffWithTestResources(String testCase) throws Exception {
        // Load test resources
        Map<String, List<CdcCheckpointDao.CheckpointData>> before = loadCheckpointData(testCase, "before.json");
        Map<String, List<CdcCheckpointDao.CheckpointData>> after = loadCheckpointData(testCase, "after.json");
        String expectedDiffJson = readResourceFile(testCase + "/expected.json");
        
        // Set up the data stream with the 'before' state
        // Create the update
        CdcAgentsDataStream cdcAgentsDataStream = new CdcAgentsDataStream();
        cdcAgentsDataStream.setCdcContent(before);
        DataStreamService.CdcAgentsDataStreamUpdate update =
            new DataStreamService.CdcAgentsDataStreamUpdate(after, cdcAgentsDataStream);
        
        // Process the diff
        diffService.processDiff(update, cdcAgentsDataStream, cdcAgentsDataStream.getCdcContent())
                .ifPresent(c -> update.beforeUpdate().getCdcCheckpointDiffs().add(c));


        // Verify we have a diff
        List<CheckpointDataDiff> checkpointDiffs = update.beforeUpdate().getCdcCheckpointDiffs();
        assertEquals(1, checkpointDiffs.size());
        
        // Convert the actual diff to JSON for comparison
        CheckpointDataDiff actualDiff = checkpointDiffs.getFirst();
        String actualDiffJson = objectMapper.writeValueAsString(actualDiff);

        log.info("Found actual diff: {}", actualDiffJson);

        // Compare expected and actual JSON (strict mode false to ignore ordering)
        JSONAssert.assertEquals(expectedDiffJson, actualDiffJson, false);

    }


    /**
     * Helper method to create checkpoint data
     */
    private CdcCheckpointDao.CheckpointData createCheckpointData(String taskId, String content, Timestamp timestamp) {
        return new CdcCheckpointDao.CheckpointData(
                content.getBytes(StandardCharsets.UTF_8),
                timestamp,
                "",
                "",
                taskId
        );
    }

    /**
     * Load checkpoint data from a JSON test resource file
     */
    private Map<String, List<CdcCheckpointDao.CheckpointData>> loadCheckpointData(String testCase, String filename)
            throws IOException {
        String path = testCase + "/" + filename;
        String json = readResourceFile(path);

        // Parse the JSON into a format we can convert to CheckpointData
        Map<String, List<Map<String, Object>>> parsed =
            objectMapper.readValue(json, HashMap.class);

        // Convert to CheckpointData objects
        Map<String, List<CdcCheckpointDao.CheckpointData>> result = new HashMap<>();

        for (Map.Entry<String, List<Map<String, Object>>> entry : parsed.entrySet()) {
            List<CdcCheckpointDao.CheckpointData> dataList = entry.getValue().stream()
                .map(map -> {
                    String content = (String) map.get("content");
                    var timestamp = Timestamp.from(Instant.ofEpochMilli(Long.valueOf(String.valueOf(map.get("timestamp")))));
                    return createCheckpointData(entry.getKey(), content, timestamp);
                })
                .collect(Collectors.toList());

            result.put(entry.getKey(), dataList);
        }

        return result;
    }

    /**
     * Read a resource file as a string
     */
    private String readResourceFile(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        return new String(Files.readAllBytes(Paths.get(resource.getURI())), StandardCharsets.UTF_8);
    }

    /**
     * Reconstruct the 'after' state by applying diffs to the 'before' state
     */
    private Map<String, List<CdcCheckpointDao.CheckpointData>> reconstructAfterState(
            Map<String, List<CdcCheckpointDao.CheckpointData>> before,
            CheckpointDataDiff diff) {

        // Create a deep copy of the 'before' state
        Map<String, List<CdcCheckpointDao.CheckpointData>> result = new HashMap<>();
        for (Map.Entry<String, List<CdcCheckpointDao.CheckpointData>> entry : before.entrySet()) {
            result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }

        // Apply each diff operation
        for (Map.Entry<String, CheckpointDataDiff.CheckpointDataDiffItem> entry : diff.getDiffData().entrySet()) {
            String taskId = entry.getKey();
            CheckpointDataDiff.CheckpointDataDiffItem diffItem = entry.getValue();

            // Handle add/remove/modify based on content change type
            for (CheckpointDataDiff.ContentChangeDiff contentChange : diffItem.changes()) {
                Git.ContentChange change = contentChange.change();
                var timestamp = contentChange.timestamp();

                if (change instanceof Git.ContentChange.InsertContent) {
                    // Handle insertion (new content or adding to existing)
                    Git.ContentChange.InsertContent insert = (Git.ContentChange.InsertContent) change;
                    String content = String.join("\n", insert.lines());

                    if (!result.containsKey(taskId)) {
                        // New task
                        result.put(taskId, List.of(createCheckpointData(taskId, content, timestamp)));
                    } else {
                        // Append or insert into existing content
                        String existingContent = new String(result.get(taskId).get(0).checkpoint(), StandardCharsets.UTF_8);
                        String[] lines = existingContent.split("\n", -1);

                        // Apply insertion at the right position
                        List<String> newLines = new ArrayList<>(Arrays.asList(lines));
                        int position = insert.linesToAdd().start();
                        newLines.addAll(position, insert.lines());

                        String newContent = String.join("\n", newLines);
                        result.put(taskId, List.of(createCheckpointData(taskId, newContent, timestamp)));
                    }
                }
                else if (change instanceof Git.ContentChange.RemoveContent) {
                    // Handle removal
                    Git.ContentChange.RemoveContent remove = (Git.ContentChange.RemoveContent) change;

                    if (result.containsKey(taskId)) {
                        // Check if this is a complete removal
                        if (remove.linesRemoved().start() == 0 &&
                            remove.lines().size() == new String(result.get(taskId).get(0).checkpoint(), StandardCharsets.UTF_8).split("\n", -1).length) {
                            result.remove(taskId);
                        } else {
                            // Partial removal
                            String existingContent = new String(result.get(taskId).get(0).checkpoint(), StandardCharsets.UTF_8);
                            String[] lines = existingContent.split("\n", -1);

                            // Remove lines from the specified range
                            List<String> newLines = new ArrayList<>(Arrays.asList(lines));
                            int start = remove.linesRemoved().start();
                            int end = remove.linesRemoved().end();

                            // Handle range removal
                            for (int i = end - 1; i >= start; i--) {
                                if (i < newLines.size()) {
                                    newLines.remove(i);
                                }
                            }

                            String newContent = String.join("\n", newLines);
                            result.put(taskId, List.of(createCheckpointData(taskId, newContent, timestamp)));
                        }
                    }
                }
                else if (change instanceof Git.ContentChange.ReplaceContent) {
                    // Handle replacement
                    Git.ContentChange.ReplaceContent replace = (Git.ContentChange.ReplaceContent) change;

                    if (result.containsKey(taskId)) {
                        // Apply removal then insertion
                        String existingContent = new String(result.get(taskId).get(0).checkpoint(), StandardCharsets.UTF_8);
                        String[] lines = existingContent.split("\n", -1);

                        // First remove content
                        List<String> newLines = new ArrayList<>(Arrays.asList(lines));
                        int start = replace.toRemove().linesRemoved().start();
                        int end = replace.toRemove().linesRemoved().end();

                        // Handle range removal
                        for (int i = end - 1; i >= start; i--) {
                            if (i < newLines.size()) {
                                newLines.remove(i);
                            }
                        }

                        // Then insert new content
                        newLines.addAll(start, replace.toAddContent().lines());

                        String newContent = String.join("\n", newLines);
                        result.put(taskId, List.of(createCheckpointData(taskId, newContent, timestamp)));
                    }
                }
            }
        }

        return result;
    }

    /**
     * Normalize checkpoint data for comparison (converts to a map of task IDs to content strings)
     */
    private Map<String, String> normalizeCheckpointData(Map<String, List<CdcCheckpointDao.CheckpointData>> data) {
        Map<String, String> normalized = new HashMap<>();

        for (Map.Entry<String, List<CdcCheckpointDao.CheckpointData>> entry : data.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                String content = new String(entry.getValue().get(0).checkpoint(), StandardCharsets.UTF_8);
                normalized.put(entry.getKey(), content);
            }
        }

        return normalized;
    }
}