package com.hayden.cdcagentsdatastream.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "cdc-subscriber-props")
@Component
@Data
public class CdcSubscriberConfigProps {

    List<Path> testRunnerPaths = new ArrayList<>();

}
