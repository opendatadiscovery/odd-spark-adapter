package org.opendatadiscovery.adapters.spark.execution;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpExecutionContext extends AbstractExecutionContext {
    private final String oddPlatformUrl;

    public HttpExecutionContext(final String applicationName,
                                final String hostName,
                                final String oddPlatformUrl) {
        super(applicationName, hostName);
        this.oddPlatformUrl = oddPlatformUrl;
    }

    @Override
    public void reportApplicationEnd() {
    }
}