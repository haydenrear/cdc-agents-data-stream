package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.commitdiffmodel.entity.CommitDiffContext;
import com.hayden.commitdiffmodel.parsediffs.StagedFileService;
import com.hayden.commitdiffmodel.repo.CdcChatTraceRepository;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.*;

@Component
@Slf4j
public class GitEnvironmentContextProvider implements ContextProvider {

    @Autowired
    private CdcAgentsDataStreamRepository dataStreamRepository;
    @Autowired(required = false)
    private StagedFileService stagedFileService;
    @Autowired
    private CdcChatTraceRepository cdcChatTraceRepository;
    @Autowired
    private DbDataSourceTrigger trigger;

    /**
     * Creates or updates an environment context item for the given session ID.
     * This collects information about git repositories and staged changes.
     *
     * @return the created or withCtx environment context item
     */
    public Optional<DataStreamContextItem> retrieveCtx(CdcAgentsDataStream stream){
        String sessionId = stream.getSessionId();
        try {
            // Find the associated CommitDiffContext
            CommitDiffContext commitDiffContext = null;

            // Try to find the context using the CdcChatTraceRepository
//            var foundContext = trigger.doWithKey(sKe -> {
//                var beforeUpdate = sKe.curr();
//                sKe.setKey("cdc-server");
//
//                var foundTrace = cdcChatTraceRepository.findBy(
//                        QCdcChatTrace.cdcChatTrace.commitDiffContext.sessionKey.eq(sessionId),
//                        q -> q.sortBy(Sort.by(QCdcChatTrace.cdcChatTrace.updatedTime.getMetadata().getName()).descending()).first());
//
//                sKe.setKey(beforeUpdate);
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

            // Save the withCtx context
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

}
