package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.hayden.cdcagentsdatastream.config.CdcSubscriberConfigProps;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.utilitymodule.stream.StreamUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Stream;

@Component
@Slf4j
@RequiredArgsConstructor
public class TestReportContextProvider implements ContextProvider {

    private final CdcSubscriberConfigProps subscriberConfigProps;

    @Override
    public Optional<DataStreamContextItem> retrieveCtx(CdcAgentsDataStream stream) {
        String sessionId = stream.getSessionId();
        try {
            // Find all test report files from configurations
            Map<String, String> reportFiles = new HashMap<>();

            // Collect reports from each registered runner that has reporting paths
            for (var reportDir : subscriberConfigProps.getTestRunnerPaths()) {
                collectReportFiles(reportDir, reportFiles, stream.getSessionId());
            }

            if (reportFiles.isEmpty()) {
                log.debug("No test report files found for session {}", sessionId);
                return Optional.empty();
            }

            // Create context item with the report files
            TestReportContextItem contextItem = TestReportContextItem.builder()
                    .sessionId(sessionId)
                    .creationTime(LocalDateTime.now())
                    .testReports(reportFiles)
                    .build();

            // Delete processed files to avoid re-processing
            deleteProcessedFiles(stream.getSessionId());

            return Optional.of(contextItem);
        } catch (Exception e) {
            log.error("Error creating test report context for session {}: {}", sessionId, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Collects test report files from the specified directory
     *
     * @param reportDir      directory to search for report files
     * @param reportFiles    map to populate with found report files
     * @param registrationId the ID of the registration that generated these reports
     */
    private void collectReportFiles(Path reportDir, Map<String, String> reportFiles, String registrationId) {
        if (!Files.exists(reportDir)) {
            log.debug("Report directory does not exist: {}", reportDir);
            return;
        }

        try (Stream<Path> paths = Files.list(reportDir)) {
            paths.filter(Files::isRegularFile)
                    .forEach(path -> {
                        try {
                            String content = Files.readString(path);
                            String fileName = path.getFileName().toString();
                            reportFiles.put(registrationId + ":" + fileName, content);
                        } catch (IOException e) {
                            log.error("Error reading report file {}: {}", path, e.getMessage(), e);
                        }
                    });
        } catch (IOException e) {
            log.error("Error listing report directory {}: {}", reportDir, e.getMessage(), e);
        }
    }

    /**
     * Deletes processed report files to avoid re-processing
     *
     * @param reportFiles map of report files that have been processed
     */
    private void deleteProcessedFiles(String sessionId) {
        for (var reportDir : subscriberConfigProps.getTestRunnerPaths()) {
            var filePath = reportDir.resolve(sessionId);

            StreamUtil.toStream(filePath.toFile().listFiles()).forEach(toDelete -> {
                try {
                    Files.deleteIfExists(toDelete.toPath());
                    log.debug("Deleted processed report file: {}", toDelete);
                } catch (IOException e) {
                    log.error("Error deleting report file {}: {}", filePath, e.getMessage(), e);
                }
            });
        }
    }
}