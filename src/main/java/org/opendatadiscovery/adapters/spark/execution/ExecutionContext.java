package org.opendatadiscovery.adapters.spark.execution;

import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerJobEnd;

public interface ExecutionContext {
    void reportSparkRddJob(final ActiveJob activeJob);

    void reportSparkSQLJob(final long executionId);

    void reportJobEnd(final SparkListenerJobEnd jobEnd);

    void reportApplicationEnd();
}
