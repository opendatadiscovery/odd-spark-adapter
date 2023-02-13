package org.opendatadiscovery.adapters.spark.execution;

import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.adapters.spark.dto.ExecutionPayload;
import org.opendatadiscovery.client.model.DataEntityList;

@Slf4j
public class LoggingExecutionContext extends AbstractExecutionContext {
    public LoggingExecutionContext(final ExecutionPayload executionPayload) {
        super(executionPayload);
    }

    @Override
    public void reportApplicationEnd() {
        final DataEntityList dataEntityList = buildODDModels();

        log.info("Result data entity list: {}", dataEntityList);
    }
}
