package org.opendatadiscovery.adapters.spark.execution;

import org.apache.spark.scheduler.ActiveJob;

public interface ExecutionContext {
    void reportSparkRddJob(final ActiveJob activeJob);

    void reportSparkSQLJob(final long executionId);

    void reportApplicationEnd();
}
