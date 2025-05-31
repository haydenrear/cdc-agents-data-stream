package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.hayden.cdcagentsdatastream.config.CdcSubscriberConfigProps;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.persistence.lock.AdvisoryLock;
import com.hayden.utilitymodule.MapFunctions;
import com.hayden.utilitymodule.io.FileUtils;
import com.hayden.utilitymodule.stream.StreamUtil;
import jakarta.persistence.PersistenceException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
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
    private final AdvisoryLock advisoryLock;

    @Override
    public Optional<DataStreamContextItem> retrieveCtx(CdcAgentsDataStream stream) {
        String sessionId = stream.getSessionId();
        // Find all test report files from configurations
        var reportFilesOut = MapFunctions.CollectMap(
                subscriberConfigProps.getTestRunnerPaths()
                        .stream().parallel()
                        .flatMap(reportDir -> {
                            Path resolve = reportDir.resolve(sessionId);
                            if (!resolve.toFile().exists()) {
                                return Stream.empty();
                            }

                            var sessionDir = resolve.toFile().getAbsolutePath();

                            try {
                                log.info("Retrieving report files from {}", sessionDir);
                                advisoryLock.doLock(sessionDir, "function-calling");
                                var reportFiles = collectReportFiles(reportDir, stream.getSessionId());

                                if (reportFiles.isEmpty()) {
                                    log.debug("No test report files found for session {}", sessionId);
                                    return Stream.empty();
                                }

                                // Delete processed files to avoid re-processing
                                deleteProcessedFiles(stream.getSessionId());

                                return reportFiles.entrySet().stream();
                            } finally {
                                doUnlockRecursive(sessionDir);
                            }
                        }));

        return Optional.of(
                TestReportContextItem.builder()
                        .sessionId(sessionId)
                        .creationTime(LocalDateTime.now())
                        .testReports(reportFilesOut)
                        .build());
    }

    private void doUnlockRecursive(String sessionDir) {
        try {
            advisoryLock.doUnlock(sessionDir, "function-calling");
        } catch (DataAccessException |
                 PersistenceException e) {
            log.error("Failed to unlock session {} - will try again indefinitely.", sessionDir, e);
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                log.error("Error waiting to unlock session {}", sessionDir, ex);
            }
            doUnlockRecursive(sessionDir);
        }
    }

    /**
     * Collects test report files from the specified directory
     *
     * @param reportDir      directory to search for report files
     * @param registrationId the ID of the registration that generated these reports
     * @return
     */
    private Map<String, String> collectReportFiles(Path reportDir, String registrationId) {
        Map<String, String> reportFiles = new HashMap<>();
        if (!Files.exists(reportDir)) {
            log.debug("Report directory does not exist: {}", reportDir);
            return reportFiles;
        }

        // TODO: should zip the whole parent directory and save.
        FileUtils.doOnFilesRecursive(reportDir.resolve(registrationId), path -> {
            try {
                if (path.toFile().isFile()) {
                    String content = Files.readString(path);
                    String fileName = path.getFileName().toString();
                    reportFiles.put(registrationId + ":" + fileName, content);
                }
            } catch (IOException e) {
                log.error("Error reading report file {}: {}", path, e.getMessage(), e);
            }

            return true;
        });

        return reportFiles;
    }

    /**
     * Deletes processed report files to avoid re-processing
     */
    private void deleteProcessedFiles(String sessionId) {
        for (var reportDir : subscriberConfigProps.getTestRunnerPaths()) {
            var filePath = reportDir.resolve(sessionId);
            StreamUtil.toStream(filePath.toFile().listFiles())
                    .forEach(toDelete -> {
                        try {
                            if (toDelete.isFile()) {
                                Files.deleteIfExists(toDelete.toPath());
                            } else if (toDelete.isDirectory()) {
                                FileUtils.deleteFilesRecursive(toDelete.toPath());
                                log.debug("Deleted processed report file: {}", toDelete);
                            }
                        } catch (IOException e) {
                            log.error("Error deleting report file {}: {}", filePath, e.getMessage(), e);
                        }
                    });
        }
    }
}