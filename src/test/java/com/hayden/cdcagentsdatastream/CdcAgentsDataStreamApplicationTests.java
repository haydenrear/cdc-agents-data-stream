package com.hayden.cdcagentsdatastream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.cdcagentsdatastream.model.BaseMessage;
import com.hayden.cdcagentsdatastream.repository.CdcAgentsDataStreamRepository;
import com.hayden.cdcagentsdatastream.service.CdcAgentsDataStreamService;
import com.hayden.cdcagentsdatastream.subscriber.CdcAgentsPostgresSubscriber;
import com.hayden.utilitymodule.MapFunctions;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Slf4j
@SpringBootTest
@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
class CdcAgentsDataStreamApplicationTests {

    @Autowired
    DataSource dataSource;
    @Autowired
    CdcAgentsPostgresSubscriber cdcAgentsPostgresSubscriber;
    @Autowired
    private DbDataSourceTrigger dbDataSourceTrigger;
    @Autowired
    private CdcAgentsDataStreamRepository cdcAgentsDataStreamRepository;

    @Autowired
    CheckpointDao dao;
    @Autowired
    CdcAgentsDataStreamService s;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    ObjectMapper objectMapper;

    public record NextDataItem(List<BaseMessage> messages, String task) {}

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

    public static Map<String, NextDataItem> testData(int num) {
        return MapFunctions.CollectMap(IntStream.range(0, 5).boxed().map("%s_task"::formatted)
                .map(i -> {
                    if (num == 3)  {
                        return createMessages(2, i);
                    } else {
                        return createMessages(num, i);
                    }
                }));
    }

    private static @NotNull Map.Entry<String, NextDataItem> createMessages(int endExclusive, String i) {
        List<BaseMessage> k =  IntStream.range(0, endExclusive).boxed()
                .map(j -> {
                    BaseMessage b = new BaseMessage.AIMessage("hello!!!");
                    return b;
                })
                .toList();
        return Map.entry(i, new NextDataItem(k, i));
    }

    private static CyclicBarrier specialCyclicBarrier = new CyclicBarrier(2);

    @SneakyThrows
    @Test
    public void testDao() {
        AtomicInteger i = new AtomicInteger();
        final String test = UUID.randomUUID().toString();
        SynchronousQueue<String> checkpoints = new SynchronousQueue<>();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            dbDataSourceTrigger.doWithKey(sKey -> {
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
                                 'test_task',
                                 0,
                                 'messages',
                                 'list',
                                 convert_to('%s', 'UTF-8'),
                                 'test_task'
                            );
                    """;
                    try {
                        var messages = objectMapper.writeValueAsString(e.getValue().messages);
                        var sqlInsert = insertValue.formatted(test, checkpointId, Timestamp.from(Instant.now()), test, checkpointId, messages);
                        jdbcTemplate.execute(sqlInsert);
                        checkpoints.add(checkpointId);
                        log.info("Adding {}!", checkpointId);
                        specialCyclicBarrier.await();
                    } catch (Exception exc) {
                        log.error(exc.getMessage(), e);
                    }
                }
            });
        }, 0, 500, TimeUnit.MILLISECONDS);

        int num = 0;

        LocalDateTime time = null;

        while (num < 20) {
            var nextCheckpoint = checkpoints.poll(600, TimeUnit.MILLISECONDS);
            log.info("Checkpoint {}", nextCheckpoint);

            Optional<CdcAgentsDataStream> bySessionId = cdcAgentsDataStreamRepository.findBySessionId(test);
            while (bySessionId.isEmpty()) {
                bySessionId = cdcAgentsDataStreamRepository.findBySessionId(test);
                Thread.sleep(30);
            }

            bySessionId = cdcAgentsDataStreamRepository.findBySessionId(test);

            if (time == null) {
                time = bySessionId.get().getUpdatedTime();
            } else if (time.isBefore(bySessionId.get().getUpdatedTime())) {
                num += 1;
                time = bySessionId.get().getUpdatedTime();
            }

            specialCyclicBarrier.await();
        }

        assertThat(num).isEqualTo(20);
    }

}
