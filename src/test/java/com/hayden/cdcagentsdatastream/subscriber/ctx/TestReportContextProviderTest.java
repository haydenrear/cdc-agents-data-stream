package com.hayden.cdcagentsdatastream.subscriber.ctx;

import com.hayden.cdcagentsdatastream.config.CdcSubscriberConfigProps;
import com.hayden.cdcagentsdatastream.entity.CdcAgentsDataStream;
import com.hayden.persistence.lock.AdvisoryLock;
import com.hayden.utilitymodule.db.DbDataSourceTrigger;
import com.hayden.utilitymodule.io.FileUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.nio.file.Path;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest
@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
class TestReportContextProviderTest {

    @Autowired
    private TestReportContextProvider testReportContextProvider;

    @Autowired
    private CdcSubscriberConfigProps cdcSubscriberConfigProps;

    @Autowired
    private AdvisoryLock advisoryLock;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private DbDataSourceTrigger trigger;

    @SneakyThrows
    @Test
    public void doTest() {
        var found = cdcSubscriberConfigProps.getTestRunnerPaths().getFirst();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Path path = found;
        FileUtils.deleteFilesRecursive(path);
        path = path.resolve("test-registration");
        FileUtils.copyAll(resolver.getResource("classpath:sample_to_copy").getFile().toPath(), path);
        var test = path.resolve("one.txt");
        assertThat(test.toFile().exists()).isTrue();
        var retrieved = testReportContextProvider.retrieveCtx(CdcAgentsDataStream.builder().sessionId("test-registration").build());
        assertNotNull(retrieved);
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get()).isInstanceOf(TestReportContextItem.class);
        var tr = (TestReportContextItem) retrieved.get();
        assertThat(tr.getTestReports().size()).isEqualTo(2);
        assertThat(test.toFile().exists()).isFalse();
    }

    @Test
    public void advisory() {
        trigger.doWithKey(sKey -> {
            sKey.setKey("function-calling");
            advisoryLock.printAdvisoryLocks("function-calling");
            advisoryLock.doLock("hello!", jdbcTemplate);
            advisoryLock.printAdvisoryLocks("function-calling");
            advisoryLock.doUnlock("hello!", jdbcTemplate);
            advisoryLock.printAdvisoryLocks("function-calling");
        });
    }

}