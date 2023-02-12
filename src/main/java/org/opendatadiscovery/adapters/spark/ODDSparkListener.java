package org.opendatadiscovery.adapters.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.opendatadiscovery.adapters.spark.execution.ExecutionContext;
import org.opendatadiscovery.adapters.spark.execution.ExecutionContextFactory;
import org.opendatadiscovery.adapters.spark.utils.SparkUtils;

import java.util.Optional;

@Slf4j
public class ODDSparkListener extends SparkListener {
    private ExecutionContext executionContext;

    @Override
    public void onApplicationStart(final SparkListenerApplicationStart appStartEvent) {
        log.info("Creating ExecutionContext for application: id: {}, name: {}",
            appStartEvent.appId().get(),
            appStartEvent.appName()
        );
        this.executionContext = ExecutionContextFactory.create(appStartEvent.appName());
    }

    @Override
    public void onJobStart(final SparkListenerJobStart jobStartEvent) {
        log.info("Job has started: {}", jobStartEvent);

        final Optional<Long> executionId = Optional
            .ofNullable(jobStartEvent.properties().getProperty(SQLExecution.EXECUTION_ID_KEY()))
            .map(Long::parseLong);

        if (executionId.isPresent()) {
            executionContext.reportSparkSQLJob(executionId.get());
        } else {
            SparkUtils.getActiveJob(jobStartEvent.jobId()).ifPresent(executionContext::reportSparkRddJob);
        }
    }

    @Override
    public void onOtherEvent(final SparkListenerEvent event) {
        if (event instanceof SparkListenerSQLExecutionStart) {
            executionContext.reportSparkSQLJob(((SparkListenerSQLExecutionStart) event).executionId());
        }
    }

    @Override
    public void onJobEnd(final SparkListenerJobEnd jobEndEvent) {
        log.info("Job {} has ended", jobEndEvent);
        executionContext.reportJobEnd(jobEndEvent);
    }

    @Override
    public void onApplicationEnd(final SparkListenerApplicationEnd applicationEnd) {
        executionContext.reportApplicationEnd();
    }
}