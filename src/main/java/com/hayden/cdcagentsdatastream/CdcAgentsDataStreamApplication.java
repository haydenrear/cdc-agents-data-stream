package com.hayden.cdcagentsdatastream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.export.otlp.OtlpMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.ai.vectorstore.pgvector.autoconfigure.PgVectorStoreAutoConfiguration;

@SpringBootApplication
public class CdcAgentsDataStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(CdcAgentsDataStreamApplication.class, args);
    }

}
