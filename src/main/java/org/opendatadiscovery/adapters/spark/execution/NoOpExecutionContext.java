package org.opendatadiscovery.adapters.spark.execution;

import org.apache.spark.scheduler.ActiveJob;

public class NoOpExecutionContext implements ExecutionContext {
    @Override
    public void reportSparkRddJob(final ActiveJob activeJob) {
    }

    @Override
    public void reportSparkSQLJob(final long executionId) {
    }

    @Override
    public void reportApplicationEnd() {
    }
}
