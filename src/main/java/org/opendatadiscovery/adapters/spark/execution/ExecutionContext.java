package org.opendatadiscovery.adapters.spark.execution;

import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerJobEnd;

import java.util.Properties;

public interface ExecutionContext {
    void reportSparkRddJob(final ActiveJob activeJob);

    void reportSparkSQLJob(final long executionId, final Properties properties);

    void reportJobEnd(final SparkListenerJobEnd jobEnd);

    void reportApplicationEnd();
}
