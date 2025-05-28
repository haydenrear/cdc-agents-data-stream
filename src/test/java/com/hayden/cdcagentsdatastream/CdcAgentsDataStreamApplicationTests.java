package com.hayden.cdcagentsdatastream;

import com.hayden.cdcagentsdatastream.dao.CheckpointDao;
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

    @Test
    public void testDao() {
        var found = dao.retrieveAndStoreCheckpoint("0f76c937-ec5d-45b7-b72f-798887989b5b", "1f03345f-70b2-60d6-8000-37341710f457");
        System.out.println();
    }

}
