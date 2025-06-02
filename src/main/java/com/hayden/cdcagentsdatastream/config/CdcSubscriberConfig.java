package com.hayden.cdcagentsdatastream.config;

import com.google.common.collect.Lists;
import com.hayden.cdcagentsdatastream.dao.CdcCheckpointDao;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.dao.IdeCheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.cdcagentsdatastream.service.CdcAgentsDataStreamService;
import com.hayden.cdcagentsdatastream.service.IdeDataStreamService;
import com.hayden.commitdiffcontext.config.CommitDiffContextConfig;
import com.hayden.commitdiffmodel.config.CommitDiffContextDisableLoggingConfig;
import com.hayden.commitdiffmodel.config.CommitDiffContextTelemetryLoggingConfig;
import com.hayden.persistence.cdc.CdcSubscriber;
import com.hayden.persistence.config.CdcConfig;
import com.hayden.persistence.lock.AdvisoryLock;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.sql.init.SqlDataSourceScriptDatabaseInitializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.boot.sql.init.DatabaseInitializationSettings;
import org.springframework.context.annotation.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

@ComponentScan(
        basePackageClasses = {
                CdcConfig.class, CdcSubscriber.class, CommitDiffContextConfig.class
        },
        basePackages = "com.hayden.commitdiffcontext")
@Import({CommitDiffContextTelemetryLoggingConfig.class, CommitDiffContextDisableLoggingConfig.class, DbDataSourceTrigger.class,
        AdvisoryLock.class})
@EnableJpaRepositories(basePackageClasses = {CdcAgentsDataStreamRepository.class},
                       basePackages = "com.hayden.commitdiffcontext")
@EntityScan(basePackageClasses = CdcAgentsDataStream.class,
            basePackages = "com.hayden.commitdiffmodel")
@Configuration
@Slf4j
public class CdcSubscriberConfig {


    @Bean
    @ConfigurationProperties("spring.datasource.cdc-subscriber")
    public DataSource cdcSubscriberDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.cdc-server")
    public DataSource cdcServerDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.cdc-data-stream")
    public DataSource cdcDataStreamDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.function-calling")
    public DataSource cdcFunctionCallingLock() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @Primary
    public DataSource dataSource(DbDataSourceTrigger dbDataSourceTrigger) {
        dbDataSourceTrigger.initializeKeyTo("cdc-data-stream");
        AbstractRoutingDataSource routingDataSource = new AbstractRoutingDataSource() {
            @Override
            protected Object determineCurrentLookupKey() {
                var curr = dbDataSourceTrigger.currentKey();
                String found = null;
                if (TransactionSynchronizationManager.hasResource("data-source-key")) {
                    found = (String) TransactionSynchronizationManager.getResource("data-source-key");
                }
                return found == null ? curr : found;
            }
        };

        Map<Object, Object> resolvedDataSources = new HashMap<>();
        resolvedDataSources.put("cdc-data-stream", cdcDataStreamDataSource());
        resolvedDataSources.put("cdc-server", cdcServerDataSource());
        resolvedDataSources.put("cdc-subscriber", cdcSubscriberDataSource());
        resolvedDataSources.put("function-calling", cdcFunctionCallingLock());

        routingDataSource.setTargetDataSources(resolvedDataSources);
        routingDataSource.setDefaultTargetDataSource(cdcDataStreamDataSource());

        routingDataSource.afterPropertiesSet();

        return routingDataSource;
    }

    @Bean
    public CommandLineRunner checkLocks(AdvisoryLock advisoryLock) {
        advisoryLock.scheduleAdvisoryLockLogger("function-calling");
        return args -> {};
    }

    @Bean
    @DependsOn("initializeDataSource")
    public CommandLineRunner kickstart(CdcAgentsDataStreamService cdcStreamService,
                                       IdeDataStreamService ideStreamService,
                                       CdcCheckpointDao checkpointDao,
                                       IdeCheckpointDao ideCheckpointDao,
                                       CdcAgentsDataStreamRepository dataStreamRepository,
                                       DbDataSourceTrigger trigger) {
        return args -> {
            log.info("Starting checkpoint data processing...");

            var processedNum = trigger.doOnKey(sKey -> {
                sKey.setKey("cdc-subscriber");
                return doSave(cdcStreamService::doReadStreamItem, checkpointDao, dataStreamRepository, trigger);
            });


            log.info("Finished CDC checkpoint data processing - processed {} checkpoints into db.", processedNum);

            processedNum = trigger.doOnKey(sKey -> {
                sKey.setKey("cdc-subscriber");
                return doSave(ideStreamService::doReadStreamItem, ideCheckpointDao, dataStreamRepository, trigger);
            });

            log.info("Finished IDE checkpoint data processing - processed {} checkpoints into db.", processedNum);

        };
    }

    private static int doSave(BiFunction<String, String, Optional<CdcAgentsDataStream>> streamService,
                              CheckpointDao checkpointDao,
                              CdcAgentsDataStreamRepository dataStreamRepository,
                              DbDataSourceTrigger trigger) {
        var latestCheckpoints = checkpointDao.queryLatestCheckpoints();

        log.info("Found {} thread_ids with latest checkpoints to process", latestCheckpoints.size());

        int processedNum = 0;

        for (var checkpoint : latestCheckpoints) {
            String threadId = checkpoint.threadId();
            String checkpointId = checkpoint.checkpointId();
            String taskId = checkpoint.taskPath();
            Timestamp checkpointTimestamp = checkpoint.timestamp();


            Optional<CdcAgentsDataStream> existingDataStream = trigger.doOnKey(sKey -> {
                 sKey.setKey("cdc-data-stream");
                 return dataStreamRepository.findBySessionId(threadId);
            });

            boolean shouldProcess = existingDataStream.isEmpty() || !checkpointDao.skipParsingCheckpoint(existingDataStream.get(), taskId, checkpointTimestamp);

            if (shouldProcess) {
                log.info("Processing latest checkpoint data for thread_id={}, checkpoint_id={}, timestamp={}",
                         threadId, checkpointId, checkpointTimestamp);
                streamService.apply(threadId, checkpointId)
                    .ifPresentOrElse(
                        dataStream -> log.info("Successfully processed data for thread_id={}, checkpoint_id={}", threadId, checkpointId),
                        () -> log.warn("No data processed for thread_id={}, checkpoint_id={}", threadId, checkpointId));
                processedNum += 1;
            } else {
                log.info("Skipping thread_id={}, checkpoint_id={} - already have newer or equal data", threadId, checkpointId);
            }
        }
        return processedNum;
    }

    @Bean
    public CommandLineRunner initializeDataSource(DbDataSourceTrigger trigger) {
        doPerformInitializationWithTrigger(trigger, "cdc-subscriber", "cdc-agents-schema.sql");
        doPerformInitializationWithTrigger(trigger, "cdc-subscriber", "ide-schema.sql");
        return args -> {};
    }

    private void doPerformInitializationWithTrigger(DbDataSourceTrigger trigger, String dbKey, String schemaPath) {
        trigger.doWithKey(sKey -> {
            sKey.setKey(dbKey);
            doPerformInitialization(schemaPath);
        });
    }

    private void doPerformInitialization(String path) {
        DatabaseInitializationSettings settings = new DatabaseInitializationSettings();
        settings.setSchemaLocations(Lists.newArrayList(new ClassPathResource(path).getPath()));
        settings.setMode(DatabaseInitializationMode.ALWAYS);
        var i = new SqlDataSourceScriptDatabaseInitializer(cdcSubscriberDataSource(), settings);
        try {
            i.afterPropertiesSet();
            if (!i.initializeDatabase()) {
                log.error("Database initialization failed");
                throw new RuntimeException("Database initialization failed");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
