package com.hayden.cdcagentsdatastream.config;

import com.google.common.collect.Lists;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.cdcagentsdatastream.service.CdcAgentsDataStreamService;
import com.hayden.commitdiffcontext.config.CommitDiffContextConfig;
import com.hayden.commitdiffcontext.message.GitDiffCodeResponseDeser;
import com.hayden.commitdiffmodel.codegen.types.McpContext;
import com.hayden.commitdiffmodel.config.CommitDiffContextDisableLoggingConfig;
import com.hayden.commitdiffmodel.config.CommitDiffContextTelemetryLoggingConfig;
import com.hayden.commitdiffmodel.scalar.Float32Array;
import com.hayden.commitdiffmodel.scalar.ServerByteArray;
import com.hayden.persistence.cdc.CdcSubscriber;
import com.hayden.persistence.config.CdcConfig;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ComponentScan(
        basePackageClasses = {
                CdcConfig.class, CdcSubscriber.class, CommitDiffContextConfig.class
        },
        basePackages = "com.hayden.commitdiffcontext")
@Import({CommitDiffContextTelemetryLoggingConfig.class, CommitDiffContextDisableLoggingConfig.class, DbDataSourceTrigger.class})
@EnableJpaRepositories(basePackageClasses = {CdcAgentsDataStreamRepository.class},
                       basePackages = "com.hayden.commitdiffcontext")
@EntityScan(basePackageClasses = CdcAgentsDataStream.class,
            basePackages = "com.hayden.commitdiffmodel")
@Configuration
@Slf4j
public class CdcSubscriberConfig {

    @SneakyThrows
    @Bean
    public CommandLineRunner initializeDataSource() {
        DatabaseInitializationSettings settings = new DatabaseInitializationSettings();
        settings.setSchemaLocations(Lists.newArrayList(new ClassPathResource("schema.sql").getPath()));
        settings.setMode(DatabaseInitializationMode.ALWAYS);
        var i = new SqlDataSourceScriptDatabaseInitializer(cdcSubscriberDataSource(), settings);
        i.afterPropertiesSet();
        if (!i.initializeDatabase()) {
            log.error("Database initialization failed");
        }
        return args -> {};
    }

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
    @Primary
    public DataSource dataSource(DbDataSourceTrigger dbDataSourceTrigger) {
        dbDataSourceTrigger.initializeKeyTo("cdc-data-stream");
        AbstractRoutingDataSource routingDataSource = new AbstractRoutingDataSource() {
            @Override
            protected Object determineCurrentLookupKey() {
                return dbDataSourceTrigger.currentKey();
            }
        };

        Map<Object, Object> resolvedDataSources = new HashMap<>();
        resolvedDataSources.put("cdc-data-stream", cdcDataStreamDataSource());
        resolvedDataSources.put("cdc-server", cdcServerDataSource());

        routingDataSource.setTargetDataSources(resolvedDataSources);
        routingDataSource.setDefaultTargetDataSource(cdcDataStreamDataSource());

        routingDataSource.afterPropertiesSet();

        return routingDataSource;
    }

    @Bean
    public CommandLineRunner kickstart(CdcAgentsDataStreamService streamService,
                                       CheckpointDao checkpointDao,
                                       CdcAgentsDataStreamRepository dataStreamRepository) {
        return args -> {
            log.info("Starting checkpoint data processing...");

            var latestCheckpoints = checkpointDao.queryLatestCheckpoints();

            log.info("Found {} thread_ids with latest checkpoints to process", latestCheckpoints.size());

            int processedNum = 0;

            for (var checkpoint : latestCheckpoints) {
                String threadId = checkpoint.threadId();
                String checkpointId = checkpoint.checkpointId();
                java.sql.Timestamp checkpointTimestamp = checkpoint.timestamp();
                
                // Check if we already have this thread_id in our database
                Optional<CdcAgentsDataStream> existingDataStream = dataStreamRepository.findBySessionId(threadId);
                
                boolean shouldProcess = existingDataStream.isEmpty() || existingDataStream.get().getCheckpointTimestamp() == null || checkpointTimestamp.after(existingDataStream.get().getCheckpointTimestamp());
                
                if (shouldProcess) {
                    log.info("Processing latest checkpoint data for thread_id={}, checkpoint_id={}, timestamp={}", 
                             threadId, checkpointId, checkpointTimestamp);
                    streamService.doReadStreamItem(threadId, checkpointId)
                        .ifPresentOrElse(
                            dataStream -> log.info("Successfully processed data for thread_id={}, checkpoint_id={}", threadId, checkpointId),
                            () -> log.warn("No data processed for thread_id={}, checkpoint_id={}", threadId, checkpointId));
                    processedNum += 1;
                } else {
                    log.info("Skipping thread_id={}, checkpoint_id={} - already have newer or equal data", threadId, checkpointId);
                }
            }

            log.info("Finished checkpoint data processing - processed {} checkpoints into db.", processedNum);
        };
    }

}
