package com.hayden.cdcagentsdatastream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.model.BaseMessage;
import com.hayden.cdcagentsdatastream.service.CdcAgentsDataStreamService;
import com.hayden.cdcagentsdatastream.subscriber.CdcAgentsPostgresSubscriber;
import com.hayden.utilitymodule.MapFunctions;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    @SneakyThrows
    @Test
    void contextLoads() {
        Thread.sleep(1000000);
    }

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

    public static Map<String, NextDataItem> testData(int num) {
        return MapFunctions.CollectMap(IntStream.range(0, 200).boxed().map("%s_task"::formatted)
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

    @SneakyThrows
    @Test
    public void testDao() {
        AtomicInteger i = new AtomicInteger();
        final String test = UUID.randomUUID().toString();
        List<String> checkpoints = new ArrayList<>();
        Executors.newScheduledThreadPool(2).scheduleAtFixedRate(() -> {
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
                    } catch (Exception exc) {
                        log.error(exc.getMessage(), e);
                    }
                }
            });
        }, 3, 3, TimeUnit.SECONDS);

        int num = 0;

        while (num < 10) {
            if (checkpoints.isEmpty()) {
                Thread.sleep(1000);
            } else {
                var nextCheckpoint = checkpoints.removeFirst();
                var found = s.doReadStreamItem(test, nextCheckpoint);
                log.info("found: {}", found);
                if (found.isPresent()) {
                    num += 1;
                }
                Thread.sleep(1000);
            }
        }
    }

}
