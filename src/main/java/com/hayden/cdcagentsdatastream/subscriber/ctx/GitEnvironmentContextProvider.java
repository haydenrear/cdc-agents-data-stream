package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.cdcagentsdatastream.util.GitRepositoryScanner;
import com.hayden.commitdiffcontext.entity.CommitDiffContext;
import com.hayden.commitdiffcontext.parsediffs.StagedFileService;
import com.hayden.commitdiffcontext.repo.CdcChatTraceRepository;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
    @Autowired
    private GitRepositoryScanner scanner;

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
            EnvironmentContextItem.EnvironmentContextItemBuilder contextItem = EnvironmentContextItem
                    .builder()
                    .sessionId(sessionId);

//            // Collect git directory information
//            Map<String, String> gitDirectories = scanner.scanDirectoryForRepositories(null, 0);
//            contextItem.setGitDirectories(gitDirectories);
//
//            // Collect commit hashes for each git directory
//            Map<String, List<String>> commitHashes = scanner.collectCommitHashes(null, 0);
//            contextItem.setCommitHashes(commitHashes);

            // Try to get staged data if available
            if (stagedFileService != null && commitDiffContext != null) {
                // Note: This would need to be implemented based on your specific requirements
                // contextItem.setStagedData(collectStagedData(commitDiffContext));
            }

            // Save the withCtx context
            return Optional.of(contextItem.build());

        } catch (Exception e) {
            log.error("Error creating environment context for session {}: {}", sessionId, e.getMessage(), e);
            return Optional.empty();
        }
    }

}
