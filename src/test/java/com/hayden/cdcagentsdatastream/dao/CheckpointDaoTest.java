package com.hayden.cdcagentsdatastream.dao;

import com.hayden.cdcagentsdatastream.entity.QCdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.model.BaseMessage;
import com.hayden.utilitymodule.MapFunctions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest
@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@Slf4j
class CheckpointDaoTest {

    @Autowired
    private DbDataSourceTrigger dbDataSourceTrigger;
    @Autowired
    private CdcAgentsDataStreamRepository cdcAgentsDataStreamRepository;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    public record NextDataItem(List<BaseMessage> messages, String task) { }

    @BeforeEach
    public void before() {
        dbDataSourceTrigger.doWithKey(sKey -> {
            sKey.setKey("cdc-subscriber");
            jdbcTemplate.execute("""
                        DELETE FROM checkpoint_writes;
                        DELETE FROM checkpoints;
                    """);
        });
        log.info("Cleared checkpoints..");
    }

    @AfterEach
    public void after() {
        dbDataSourceTrigger.doWithKey(sKey -> {
            sKey.setKey("cdc-subscriber");
            jdbcTemplate.execute("""
                        DELETE FROM checkpoint_writes;
                        DELETE FROM checkpoints;
                    """);
        });
        log.info("Cleared checkpoints..");
    }


    @SneakyThrows
    @Test
    public void testDao() {
        CyclicBarrier specialCyclicBarrier = new CyclicBarrier(2);
        AtomicInteger i = new AtomicInteger();
        final String threadSessionId = UUID.randomUUID().toString();
        SynchronousQueue<String> checkpoints = new SynchronousQueue<>();
        Executors.newScheduledThreadPool(1)
                .scheduleAtFixedRate(() -> dbDataSourceTrigger.doWithKey(sKey -> {
                    sKey.setKey("cdc-subscriber");
                    var nextDataItems = testData(i.getAndIncrement());

                    for (var e : nextDataItems.entrySet()) {
                        String checkpointId = UUID.randomUUID().toString();
                        @Language("sql") String insertValue = """
                                        INSERT INTO checkpoints VALUES (
                                                                   '%s',
                                                                   '',
                                                                   '%s',
                                                                   NULL,
                                                                   NULL,
                                                                   '{"ts": "%s"}'::jsonb,
                                                                   '[{}]'::jsonb
                                        );
                                        INSERT INTO checkpoint_writes VALUES (
                                             '%s',
                                             '',
                                             '%s',
                                             '%s',
                                             0,
                                             'messages',
                                             'list',
                                             convert_to('%s', 'UTF-8'),
                                             '%s'
                                        );
                                """;
                        try {
                            var messages = objectMapper.writeValueAsString(e.getValue().messages);
                            var sqlInsert = insertValue.formatted(threadSessionId, checkpointId, Timestamp.from(Instant.now()), threadSessionId, checkpointId, e.getKey(), messages, e.getKey());
                            jdbcTemplate.execute(sqlInsert);
                            checkpoints.add(checkpointId);
                            log.info("Adding {}!", checkpointId);
                            specialCyclicBarrier.await();
                        } catch (Exception exc) {
                            log.error(exc.getMessage(), e);
                        }
                    }
                }), 0, 500, TimeUnit.MILLISECONDS);

        AtomicInteger num = new AtomicInteger(0);

        LocalDateTime time = null;

        while (num.get() < 20) {

            var nextCheckpoint = checkpoints.poll(600, TimeUnit.MILLISECONDS);
            log.info("Checkpoint {}", nextCheckpoint);

            Optional<CdcAgentsDataStream> bySessionId;

            while ((bySessionId = cdcAgentsDataStreamRepository.findBySessionId(threadSessionId)).isEmpty()) {
                Thread.sleep(30);
            }

            if (time == null) {
                time = bySessionId.get().getUpdatedTime();
            } else if (time.isBefore(bySessionId.get().getUpdatedTime())) {
                num.getAndIncrement();
                time = bySessionId.get().getUpdatedTime();
            }

            specialCyclicBarrier.await();
        }

        assertThat(num.get()).isEqualTo(20);
    }

    private static Map<String, NextDataItem> testData(int num) {
        return MapFunctions.CollectMap(IntStream.range(0, 5).boxed().map("%s_task"::formatted)
                .map(i -> {
                    if (num == 3) {
                        return createMessages(2, i);
                    } else {
                        return createMessages(num, i);
                    }
                }));
    }

    private static @NotNull Map.Entry<String, NextDataItem> createMessages(int endExclusive, String i) {
        List<BaseMessage> k = IntStream.range(0, endExclusive).boxed()
                .map(j -> {
                    BaseMessage b = new BaseMessage.AIMessage("hello!!!");
                    return b;
                })
                .toList();
        return Map.entry(i, new NextDataItem(k, i));
    }

}