package com.hayden.cdcagentsdatastream.subscriber;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.entity.EnvironmentContextItem;
import com.hayden.cdcagentsdatastream.model.BaseMessage;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.cdcagentsdatastream.service.MessageConverterService;
import com.hayden.commitdiffmodel.entity.CommitDiffContext;
import com.hayden.commitdiffmodel.entity.QCdcChatTrace;
import com.hayden.commitdiffmodel.parsediffs.StagedFileService;
import com.hayden.commitdiffmodel.repo.CdcChatTraceRepository;
import com.hayden.persistence.cdc.CdcSubscriber;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import lombok.extern.slf4j.Slf4j;
import org.intellij.lang.annotations.Language;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class CdcAgentsPostgresSubscriber implements CdcSubscriber {

    @Autowired
    private CdcChatTraceRepository cdcChatTraceRepository;
    @Autowired
    private DbDataSourceTrigger trigger;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private CheckpointDao dao;
    @Autowired
    private MessageConverterService messageConverterService;
    @Autowired
    private CdcAgentsDataStreamRepository dataStreamRepository;

    @Autowired(required = false)
    private StagedFileService stagedFileService;

    @Override
    @Transactional
    public void onDataChange(String tableName, String operation, Map<String, Object> data) {
        // Process checkpoint writes - store in our data model
        if (Objects.equals(operation, "cdc_checkpoint_writes")) {
            var found = data.get("cdc_checkpoint_writes");
            if (found instanceof String s) {
                try {
                    var created = objectMapper.readValue(s, new TypeReference<Map<String, String>>() {});
                    var checkpointNs = created.get("checkpoint_ns");
                    var threadId = created.get("thread_id");
                    var checkpointId = created.getOrDefault("checkpoint_id", null);
                    
                    if (threadId != null) {
                        // Retrieve and store the checkpoint data
                        List<BaseMessage> messages = dao.retrieveAndStoreCheckpoint(threadId, checkpointId)
                                .orElse(Collections.emptyList());
                        
                        if (!messages.isEmpty()) {
                            log.info("Processed {} messages for thread {} and checkpoint {}", 
                                    messages.size(), threadId, checkpointId);
                            
                            // Find or create an environment context for this session
                            createOrUpdateEnvironmentContext(threadId);
                        }
                    }
                } catch (JsonProcessingException e) {
                    log.error("Error processing checkpoint writes: {}", e.getMessage(), e);
                }
            }
        }
        
        // Process checkpoints table updates
        if (Objects.equals(operation, "cdc_checkpoints")) {
            var found = data.get("cdc_checkpoints");
            if (found instanceof String s) {
                try {
                    var created = objectMapper.readValue(s, new TypeReference<Map<String, String>>() {});
                    var threadId = created.get("thread_id");
                    
                    if (threadId != null) {
                        // Make sure we have a data stream for this thread
                        CdcAgentsDataStream dataStream = messageConverterService.findOrCreateDataStream(threadId);
                    }
                } catch (JsonProcessingException e) {
                    log.error("Error processing checkpoints: {}", e.getMessage(), e);
                }
            }
        }
        
        // Support for other tables (blobs, migrations) can be added here
    }
    
    /**
     * Creates or updates an environment context item for the given session ID.
     * This collects information about git repositories and staged changes.
     *
     * @param sessionId the session ID to create context for
     * @return the created or updated environment context item
     */
    private Optional<EnvironmentContextItem> createOrUpdateEnvironmentContext(String sessionId) {
        try {
            // Find the associated CommitDiffContext
            CommitDiffContext commitDiffContext = null;
            
            Optional<CdcAgentsDataStream> dataStream = dataStreamRepository.findBySessionId(sessionId);
                // Try to find the context using the CdcChatTraceRepository
//            var foundContext = trigger.doWithKey(sKe -> {
//                var prev = sKe.curr();
//                sKe.setKey("cdc-server");
//
//                var foundTrace = cdcChatTraceRepository.findBy(
//                        QCdcChatTrace.cdcChatTrace.commitDiffContext.sessionKey.eq(sessionId),
//                        q -> q.sortBy(Sort.by(QCdcChatTrace.cdcChatTrace.updatedTime.getMetadata().getName()).descending()).first());
//
//                sKe.setKey(prev);
//                return foundTrace.map(trace -> trace.getCommitDiffContext()).orElse(null);
//            });

            // Create or find the environment context
            EnvironmentContextItem contextItem = EnvironmentContextItem.builder().sessionId(sessionId).build();

            // Collect git directory information
            Map<String, String> gitDirectories = scanForGitDirectories();
            contextItem.setGitDirectories(gitDirectories);
            
            // Collect commit hashes for each git directory
            Map<String, List<String>> commitHashes = collectCommitHashes(gitDirectories);
            contextItem.setCommitHashes(commitHashes);
            
            // Try to get staged data if available
            if (stagedFileService != null && commitDiffContext != null) {
                // Note: This would need to be implemented based on your specific requirements
                // contextItem.setStagedData(collectStagedData(commitDiffContext));
            }
            
            // Save the updated context
            return Optional.of(contextItem);
            
        } catch (Exception e) {
            log.error("Error creating environment context for session {}: {}", sessionId, e.getMessage(), e);
            return Optional.empty();
        }
    }
    
    /**
     * Scans for git directories in common locations.
     *
     * @return a map of directory names to their paths
     */
    private Map<String, String> scanForGitDirectories() {
        Map<String, String> directories = new HashMap<>();
        
        // Get current user's home directory
        String userHome = System.getProperty("user.home");
        
        // Common locations to check
        List<String> commonLocations = Arrays.asList(
                userHome + "/git",
                userHome + "/projects",
                userHome + "/IdeaProjects",
                userHome + "/workspace"
        );
        
        for (String location : commonLocations) {
            File dir = new File(location);
            if (dir.exists() && dir.isDirectory()) {
                scanDirectoryForGitRepos(dir, directories);
            }
        }
        
        return directories;
    }
    
    /**
     * Recursively scans a directory for git repositories.
     *
     * @param directory the directory to scan
     * @param gitDirs map to populate with found git directories
     */
    private void scanDirectoryForGitRepos(File directory, Map<String, String> gitDirs) {
        File[] files = directory.listFiles();
        if (files == null) return;
        
        // Check if this directory is a git repo
        File gitDir = new File(directory, ".git");
        if (gitDir.exists() && gitDir.isDirectory()) {
            gitDirs.put(directory.getName(), directory.getAbsolutePath());
            return; // Don't recurse into git repos
        }
        
        // Check subdirectories (only one level deep to avoid excessive scanning)
        for (File file : files) {
            if (file.isDirectory()) {
                File subGitDir = new File(file, ".git");
                if (subGitDir.exists() && subGitDir.isDirectory()) {
                    gitDirs.put(file.getName(), file.getAbsolutePath());
                }
            }
        }
    }
    
    /**
     * Collects commit hashes for each git directory.
     *
     * @param gitDirectories map of git directories
     * @return map of directory names to commit hashes
     */
    private Map<String, List<String>> collectCommitHashes(Map<String, String> gitDirectories) {
        Map<String, List<String>> commitHashes = new HashMap<>();
        
        // This is a simplified implementation
        // In a real implementation, you would use JGit or process Git commands
        // to get the actual commit hashes
        
        for (Map.Entry<String, String> entry : gitDirectories.entrySet()) {
            // Placeholder - in a real implementation, you would get actual commits
            commitHashes.put(entry.getKey(), List.of("placeholder-hash-1", "placeholder-hash-2"));
        }
        
        return commitHashes;
    }

    @Override
    public List<String> getSubscriptionName() {
        return List.of("cdc_checkpoint_blobs", "cdc_checkpoint_migrations", "cdc_checkpoint_writes", "cdc_checkpoints");
    }

    @Override
    public Optional<String> createSubscription() {
        @Language("sql") String toExec = """
                                        CREATE OR REPLACE FUNCTION notify_trigger() RETURNS trigger AS
                                        $$
                                        BEGIN
                                        PERFORM pg_notify('cdc_checkpoint_blobs', json_build_object('thread_id', NEW.thread_id, 'checkpoint_ns', NEW.checkpoint_ns)::text);
                                        RETURN NEW;
                                        END;
                                        $$ LANGUAGE plpgsql;
                                        CREATE OR REPLACE TRIGGER cdc_checkpoint_blobs
                                        AFTER INSERT OR UPDATE
                                        ON checkpoint_blobs
                                        FOR EACH ROW
                                        EXECUTE FUNCTION notify_trigger();
                
                                        CREATE OR REPLACE FUNCTION notify_trigger() RETURNS trigger AS
                                        $$
                                        BEGIN
                                            PERFORM pg_notify('cdc_checkpoint_migrations', row_to_json(NEW)::text);
                                            RETURN NEW;
                                        END;
                                        $$ LANGUAGE plpgsql;
                                        CREATE OR REPLACE TRIGGER cdc_checkpoint_migrations
                                            AFTER INSERT OR UPDATE
                                            ON checkpoint_migrations
                                            FOR EACH ROW
                                        EXECUTE FUNCTION notify_trigger();
                
                                        CREATE OR REPLACE FUNCTION notify_trigger() RETURNS trigger AS
                                        $$
                                        BEGIN
                                            PERFORM pg_notify('cdc_checkpoint_writes', json_build_object('thread_id', NEW.thread_id, 'checkpoint_ns', NEW.checkpoint_ns)::text);
                                            RETURN NEW;
                                        END;
                                        $$ LANGUAGE plpgsql;
                                        CREATE OR REPLACE TRIGGER cdc_checkpoint_writes
                                            AFTER INSERT OR UPDATE
                                            ON checkpoint_writes
                                            FOR EACH ROW
                                        EXECUTE FUNCTION notify_trigger();
                
                                        CREATE OR REPLACE FUNCTION notify_trigger() RETURNS trigger AS
                                        $$
                                        BEGIN
                                            PERFORM pg_notify('cdc_checkpoints', json_build_object('thread_id', NEW.thread_id, 'checkpoint_ns', NEW.checkpoint_ns)::text);
                                            RETURN NEW;
                                        END;
                                        $$ LANGUAGE plpgsql;
                                        CREATE OR REPLACE TRIGGER cdc_checkpoints
                                            AFTER INSERT OR UPDATE
                                            ON postgres.public.checkpoints
                                            FOR EACH ROW
                                        EXECUTE FUNCTION notify_trigger();
                """;
        return Optional.of(toExec);
    }
}
