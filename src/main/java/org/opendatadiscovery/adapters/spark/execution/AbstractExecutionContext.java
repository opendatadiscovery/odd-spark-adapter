package org.opendatadiscovery.adapters.spark.execution;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobResult;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.opendatadiscovery.adapters.spark.VisitorFactoryProvider;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;
import org.opendatadiscovery.adapters.spark.mapper.DataEntityMapper;
import org.opendatadiscovery.adapters.spark.mapper.RddMapper;
import scala.Option;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opendatadiscovery.adapters.spark.utils.Utils.namespaceUri;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractExecutionContext implements ExecutionContext {
    @Getter(value = AccessLevel.PROTECTED)
    private final String applicationName;

    @Getter(value = AccessLevel.PROTECTED)
    private final String hostName;

    @Getter(value = AccessLevel.PROTECTED)
    private long jobEndTime = 0;

    @Getter(value = AccessLevel.PROTECTED)
    private String errorMessage;

    protected final Set<LogicalPlanDependencies> dependencies = Collections.synchronizedSet(new HashSet<>());
    protected final RddMapper rddMapper = new RddMapper();

    @Override
    public void reportSparkRddJob(final ActiveJob job) {
        final RDD<?> finalRDD = job.finalStage().rdd();

        final List<String> inputs = rddMapper.inputs(finalRDD)
            .stream()
            .map(input -> DataEntityMapper.map(namespaceUri(input), input.getPath()))
            .collect(Collectors.toList());

        final List<String> outputs = rddMapper.outputs(job, null)
            .stream()
            .map(input -> DataEntityMapper.map(namespaceUri(input), input.getPath()))
            .collect(Collectors.toList());

        // TODO: fix
        dependencies.add(LogicalPlanDependencies.empty());
    }

    @Override
    public void reportSparkSQLJob(final long executionId) {
        final Option<SparkContext> sparkContext = SparkContext$.MODULE$.getActive();

        if (sparkContext.isEmpty()) {
            log.error("There's no active spark context. Skipping job with id: {}", executionId);
            return;
        }

        final QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);
        if (queryExecution == null) {
            log.warn("Query execution is null. Skipping job with id: {}", executionId);
            return;
        }

        final LogicalPlan logicalPlan = queryExecution.logical();

        VisitorFactoryProvider.create(sparkContext.get()).getVisitors()
            .stream()
            .filter(v -> v.isDefinedAt(logicalPlan))
            .map(v -> v.apply(logicalPlan))
            .findFirst()
            .ifPresent(dependencies::add);
    }

    @Override
    public void reportJobEnd(final SparkListenerJobEnd jobEnd) {
        final JobResult jobResult = jobEnd.jobResult();

        if (jobEndTime < jobEnd.time()) {
            this.jobEndTime = jobEnd.time();
        }

        if (jobResult instanceof JobFailed) {
            errorMessage = ((JobFailed) jobResult).exception().getMessage();
        }
    }
}
