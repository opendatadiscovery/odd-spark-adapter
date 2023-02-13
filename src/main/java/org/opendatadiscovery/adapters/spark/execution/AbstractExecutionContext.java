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
import org.opendatadiscovery.adapters.spark.dto.ExecutionPayload;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;
import org.opendatadiscovery.adapters.spark.mapper.DataEntityMapper;
import org.opendatadiscovery.adapters.spark.mapper.RddMapper;
import org.opendatadiscovery.adapters.spark.visitor.VisitorFactoryProvider;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityList;
import org.opendatadiscovery.client.model.DataEntityType;
import org.opendatadiscovery.client.model.DataTransformer;
import org.opendatadiscovery.client.model.DataTransformerRun;
import org.opendatadiscovery.client.model.JobRunStatus;
import org.opendatadiscovery.client.model.MetadataExtension;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.oddrn.model.SparkPath;
import scala.Option;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;
import static org.opendatadiscovery.adapters.spark.utils.Utils.namespaceUri;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractExecutionContext implements ExecutionContext {
    @Getter(value = AccessLevel.PROTECTED)
    protected final ExecutionPayload executionPayload;

    @Getter(value = AccessLevel.PROTECTED)
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

        // TODO: fix
        dependencies.add(LogicalPlanDependencies.empty());
    }

    @Override
    public void reportSparkSQLJob(final long executionId, final Properties metadataChunk) {
        final Option<SparkContext> sparkContext = SparkContext$.MODULE$.getActive();

        if (sparkContext.isEmpty()) {
            log.error("There's no active spark context. Skipping job with id: {}", executionId);
            return;
        }

        if (metadataChunk != null) {
            executionPayload.appendMetadata(metadataChunk);
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

        if (executionPayload.getJobEndTime() < jobEnd.time()) {
            executionPayload.setJobEndTime(jobEnd.time());
        }

        if (jobResult instanceof JobFailed) {
            executionPayload.setErrorMessage(((JobFailed) jobResult).exception().getMessage());
        }
    }

    protected DataEntityList buildODDModels() {
        final SparkPath dataSourceOddrn = SparkPath
            .builder()
            .host(executionPayload.getHostName())
            .build();

        final SparkPath sparkJobOddrn = SparkPath
            .builder()
            .host(executionPayload.getHostName())
            .job(executionPayload.getApplicationName())
            .build();

        final SparkPath sparkJobRunOddrn = SparkPath
            .builder()
            .host(executionPayload.getHostName())
            .job(executionPayload.getApplicationName())
            .run(String.valueOf(executionPayload.getJobEndTime()))
            .build();

        final Map<String, Object> metadata = executionPayload.getMetadata()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));

        final List<String> inputs = new ArrayList<>();
        final List<String> outputs = new ArrayList<>();

        for (final LogicalPlanDependencies dependency : getDependencies()) {
            inputs.addAll(generateOddrns(dependency.getInputs()));
            outputs.addAll(generateOddrns(dependency.getOutputs()));
        }

        final DataEntity dataTransformer = new DataEntity()
            .name(executionPayload.getApplicationName())
            .oddrn(Generator.getInstance().generate(sparkJobOddrn))
            .type(DataEntityType.JOB)
            .metadata(Collections.singletonList(new MetadataExtension()
                .schemaUrl(URI.create("http://spark.adapter"))
                .metadata(metadata)))
            .dataTransformer(new DataTransformer().inputs(inputs).outputs(outputs));

        final DataEntity dataTransformerRun = new DataEntity()
            .oddrn(Generator.getInstance().generate(sparkJobRunOddrn))
            .name(String.format("%s-%s", dataTransformer.getName(), executionPayload.getJobEndTime()))
            .type(DataEntityType.JOB_RUN)
            .dataTransformerRun(new DataTransformerRun()
                .transformerOddrn(dataTransformer.getOddrn())
                .startTime(executionPayload.getJobStartTime())
                .endTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(executionPayload.getJobEndTime()), UTC))
                .status(executionPayload.getErrorMessage() == null ? JobRunStatus.SUCCESS : JobRunStatus.FAILED)
                .statusReason(executionPayload.getErrorMessage())
            );

        return new DataEntityList()
            .dataSourceOddrn(Generator.getInstance().generate(dataSourceOddrn))
            .items(Arrays.asList(dataTransformer, dataTransformerRun));
    }

    private List<String> generateOddrns(final List<? extends OddrnPath> oddrnPaths) {
        return oddrnPaths.stream()
            .map(op -> Generator.getInstance().generate(op))
            .collect(Collectors.toList());
    }
}
