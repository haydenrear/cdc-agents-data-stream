package com.hayden.cdcagentsdatastream.util;

import com.hayden.utilitymodule.stream.StreamUtil;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryBuilder;
import org.eclipse.jgit.revwalk.RevCommit;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for scanning and retrieving information from Git repositories.
 */
@Component
@Slf4j
public class GitRepositoryScanner {

    private final ExecutorService executorService = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors() / 2));

    /**
     * Scans common directories for Git repositories.
     * // TODO externalize locations and also directories to copy as resources, such as
     *      application.yml, etc, for each repository.
     *
     * @return a map of repository names to their file paths
     */
    public Map<String, String> findGitRepositories() {
        Map<String, String> repositories = new HashMap<>();
        
        // Get current user's home directory
        String userHome = System.getProperty("user.home");
        
        // Common locations to check for repositories
        List<String> commonLocations = Arrays.asList(
                userHome + "/git",
                userHome + "/projects",
                userHome + "/IdeaProjects",
                userHome + "/workspace",
                userHome + "/dev",
                userHome + "/Documents/projects",
                userHome + "/Documents/GitHub"
        );
        
        for (String location : commonLocations) {
            scanDirectoryForGitRepos(new File(location), repositories, 2); // Scan up to 2 levels deep
        }
        
        return repositories;
    }

    /**
     * Recursively scans a directory for Git repositories up to a specified depth.
     *
     * @param directory the directory to scan
     * @param gitDirs map to populate with found git directories
     * @param maxDepth maximum recursion depth
     */
    private void scanDirectoryForGitRepos(File directory, Map<String, String> gitDirs, int maxDepth) {
        if (maxDepth < 0 || !directory.exists() || !directory.isDirectory()) {
            return;
        }

        File[] files = directory.listFiles();
        if (files == null) return;
        
        // Check if this directory is a git repo
        File gitDir = new File(directory, ".git");
        if (gitDir.exists() && gitDir.isDirectory()) {
            gitDirs.put(directory.getName(), directory.getAbsolutePath());
            return; // Don't recurse into git repos
        }
        
        // Check subdirectories
        for (File file : files) {
            if (file.isDirectory()) {
                scanDirectoryForGitRepos(file, gitDirs, maxDepth - 1);
            }
        }
    }

    /**
     * Retrieves the most recent commit hashes for a repository.
     *
     * @param repoPath path to the git repository
     * @param limit maximum number of commits to retrieve
     * @return list of commit hashes
     */
    public List<String> getRecentCommitHashes(String repoPath, int limit) {
        try (Repository repository = new RepositoryBuilder()
                .setGitDir(new File(Paths.get(repoPath, ".git").toString()))
                .build();
             Git git = new Git(repository)) {
            
            return StreamUtil.toStream(git.log().setMaxCount(limit).call().iterator())
                    .map(RevCommit::getName)
                    .collect(Collectors.toList());
            
        } catch (IOException | GitAPIException e) {
            log.error("Error retrieving commit hashes from {}: {}", repoPath, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Asynchronously collects commit hashes for multiple repositories.
     *
     * @param gitDirectories map of git directory names to their paths
     * @param limit maximum number of commits to retrieve per repository
     * @return map of directory names to commit hashes
     */
    public Map<String, List<String>> collectCommitHashes(Map<String, String> gitDirectories, int limit) {
        Map<String, CompletableFuture<List<String>>> futures = new HashMap<>();
        
        // Start asynchronous tasks for each repository
        for (Map.Entry<String, String> entry : gitDirectories.entrySet()) {
            String repoName = entry.getKey();
            String repoPath = entry.getValue();
            
            futures.put(repoName, CompletableFuture.supplyAsync(
                    () -> getRecentCommitHashes(repoPath, limit),
                    executorService
            ));
        }
        
        // Collect results
        Map<String, List<String>> results = new HashMap<>();
        for (Map.Entry<String, CompletableFuture<List<String>>> entry : futures.entrySet()) {
            try {
                results.put(entry.getKey(), entry.getValue().get());
            } catch (Exception e) {
                log.error("Error collecting commit hashes for {}: {}", entry.getKey(), e.getMessage());
                results.put(entry.getKey(), Collections.emptyList());
            }
        }
        
        return results;
    }

    /**
     * Gets the current branch name for a repository.
     *
     * @param repoPath path to the git repository
     * @return the current branch name or "unknown" if it can't be determined
     */
    public String getCurrentBranch(String repoPath) {
        try (Repository repository = new RepositoryBuilder()
                .setGitDir(new File(Paths.get(repoPath, ".git").toString()))
                .build()) {
            
            return repository.getBranch();
            
        } catch (IOException e) {
            log.error("Error getting current branch for {}: {}", repoPath, e.getMessage());
            return "unknown";
        }
    }

    /**
     * Checks if a repository has uncommitted changes.
     *
     * @param repoPath path to the git repository
     * @return true if there are uncommitted changes, false otherwise
     */
    public boolean hasUncommittedChanges(String repoPath) {
        try (Repository repository = new RepositoryBuilder()
                .setGitDir(new File(Paths.get(repoPath, ".git").toString()))
                .build();
             Git git = new Git(repository)) {
            
            return !git.status().call().isClean();
            
        } catch (IOException | GitAPIException e) {
            log.error("Error checking for uncommitted changes in {}: {}", repoPath, e.getMessage());
            return false;
        }
    }

    /**
     * Gets detailed information about a repository.
     *
     * @param repoPath path to the git repository
     * @return map containing repository details
     */
    public Map<String, Object> getRepositoryDetails(String repoPath) {
        Map<String, Object> details = new HashMap<>();
        
        try (Repository repository = new RepositoryBuilder()
                .setGitDir(new File(Paths.get(repoPath, ".git").toString()))
                .build();
             Git git = new Git(repository)) {
            
            // Basic info
            details.put("name", new File(repoPath).getName());
            details.put("path", repoPath);
            details.put("branch", repository.getBranch());
            
            // Status info
            boolean hasChanges = !git.status().call().isClean();
            details.put("hasUncommittedChanges", hasChanges);
            
            // HEAD commit
            ObjectId headId = repository.resolve(Constants.HEAD);
            if (headId != null) {
                RevCommit headCommit = git.log().add(headId).setMaxCount(1).call().iterator().next();
                details.put("headCommitHash", headCommit.getName());
                details.put("headCommitMessage", headCommit.getShortMessage());
                details.put("headCommitTime", new Date(headCommit.getCommitTime() * 1000L));
            }
            
            // Remote URLs
            details.put("remotes", repository.getRemoteNames().stream()
                    .collect(Collectors.toMap(
                            remoteName -> remoteName,
                            remoteName -> repository.getConfig().getString("remote", remoteName, "url")
                    )));
            
        } catch (IOException | GitAPIException e) {
            log.error("Error getting repository details for {}: {}", repoPath, e.getMessage());
            details.put("error", e.getMessage());
        }
        
        return details;
    }

    /**
     * Scans all directories under a specified path for git repositories.
     *
     * @param basePath the base path to scan
     * @param maxDepth maximum directory depth to scan
     * @return map of repository names to their file paths
     */
    public Map<String, String> scanDirectoryForRepositories(Path basePath, int maxDepth) {
        Map<String, String> repositories = new HashMap<>();
        
        try (Stream<Path> paths = Files.walk(basePath, maxDepth)) {
            paths.filter(Files::isDirectory)
                 .filter(path -> Files.exists(path.resolve(".git")))
                 .forEach(path -> repositories.put(path.getFileName().toString(), path.toString()));
        } catch (IOException e) {
            log.error("Error scanning for repositories in {}: {}", basePath, e.getMessage());
        }
        
        return repositories;
    }

    /**
     * Checks if a repository has uncommitted changes.
     *
     * @param repoPath path to the git repository
     * @return true if there are uncommitted changes, false otherwise
     */
    public Optional retrieveUncommittedChanges(String repoPath) {
        try (Repository repository = new RepositoryBuilder()
                .setGitDir(new File(Paths.get(repoPath, ".git").toString()))
                .build();
             Git git = new Git(repository)) {

            git.diff().call();

        } catch (IOException | GitAPIException e) {
            log.error("Error checking for uncommitted changes in {}: {}", repoPath, e.getMessage());
        }
        return Optional.empty();
    }

    /**
     * Shutdown the executor service used for asynchronous operations.
     */
    public void shutdown() {
        executorService.shutdown();
    }
}