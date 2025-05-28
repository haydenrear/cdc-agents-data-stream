package com.hayden.cdcagentsdatastream;

import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
import com.hayden.cdcagentsdatastream.service.CdcAgentsDataStreamService;
import com.hayden.cdcagentsdatastream.subscriber.CdcAgentsPostgresSubscriber;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.sql.DataSource;

@SpringBootTest
@ExtendWith(SpringExtension.class)
class CdcAgentsDataStreamApplicationTests {

    @Autowired
    DataSource dataSource;
    @Autowired
    CdcAgentsPostgresSubscriber cdcAgentsPostgresSubscriber;

    @SneakyThrows
    @Test
    void contextLoads() {
        Thread.sleep(1000000);
    }

    @Autowired
    CheckpointDao dao;
    @Autowired
    CdcAgentsDataStreamService s;

    @Test
    public void testDao() {
        var found = s.retrieveAndStoreCheckpoint("977e6347-d460-4aca-8b3e-4b84578b1ef6",
                "1f03beff-983b-6a50-bfff-d2bc9ff95fe4");
        System.out.println();
    }

}
