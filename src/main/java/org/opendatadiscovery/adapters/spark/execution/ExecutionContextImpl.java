package org.opendatadiscovery.adapters.spark.execution;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
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

@RequiredArgsConstructor
@Slf4j
public class ExecutionContextImpl implements ExecutionContext {
    private final String oddPlatformUrl;

    private final Set<LogicalPlanDependencies> dependencies = Collections.synchronizedSet(new HashSet<>());
    private final RddMapper rddMapper = new RddMapper();

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

        dependencies.add(new LogicalPlanDependencies(inputs, outputs));
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
            log.debug("Query execution is null. Skipping job with id: {}", executionId);
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
    public void reportApplicationEnd() {
        log.info("Final dependencies are {}", dependencies);
    }
}