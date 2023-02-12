package org.opendatadiscovery.adapters.spark.execution;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.OffsetDateTime;

import static java.time.ZoneOffset.UTC;

@Slf4j
public class LoggingExecutionContext extends AbstractExecutionContext {
    public LoggingExecutionContext(final String applicationName, final String hostName) {
        super(applicationName, hostName);
    }

    @Override
    public void reportApplicationEnd() {
        log.info("Application name: {}", getApplicationName());
        log.info("Host name: {}", getHostName());
        log.info("Job End time: {}", OffsetDateTime.ofInstant(Instant.ofEpochMilli(getJobEndTime()), UTC));
        if (getErrorMessage() != null) {
            log.info("Error message: {}", getErrorMessage());
        }
        log.info("Final dependencies are {}", dependencies);
    }
}
