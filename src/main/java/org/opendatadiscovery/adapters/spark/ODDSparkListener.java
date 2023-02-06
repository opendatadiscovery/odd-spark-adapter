package org.opendatadiscovery.adapters.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.opendatadiscovery.adapters.spark.mapper.DataEntityMapper;
import org.opendatadiscovery.adapters.spark.mapper.RddMapper;
import org.opendatadiscovery.adapters.spark.plan.QueryPlanVisitor;
import org.opendatadiscovery.adapters.spark.utils.SparkUtils;
import org.opendatadiscovery.client.model.DataEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

@Slf4j
public class ODDSparkListener extends SparkListener {
    public static final String ODD_HOST_CONFIG_KEY = "odd.host.url";

    private final Set<String> inputs = Collections.synchronizedSet(new HashSet<>());
    private final Set<String> outputs = Collections.synchronizedSet(new HashSet<>());

    private static final Properties PROPERTIES = new Properties();

    private static final WeakHashMap<RDD<?>, Configuration> RDD_CONFIG = new WeakHashMap<>();

    public static Configuration getConfigForRDD(final RDD<?> rdd) {
        return RDD_CONFIG.get(rdd);
    }

    public static void setProperties(final String agentArgs) {
        PROPERTIES.setProperty(ODD_HOST_CONFIG_KEY, agentArgs);
    }

    @Override
    public void onApplicationEnd(final SparkListenerApplicationEnd applicationEnd) {
        log.info("Final deps: {}", this.inputs);

//        RDD_CONFIG.clear();
//        final DataEntityList dataEntityList = DataEntityMapper.map(dataEntity, inputs, outputs);
//        log.info("{}", dataEntityList);
//        final SparkConf conf = SparkEnv$.MODULE$.get().conf();
//
//        final String host = ScalaConversionUtils.findSparkConfigKey(conf, ODD_HOST_CONFIG_KEY)
//            .map(x -> PROPERTIES.getProperty(ODD_HOST_CONFIG_KEY))
//            .orElse(Utils.getProperty(System.getProperties(), ODD_HOST_CONFIG_KEY));
//
//        if (host != null) {
//            log.info("Setting ODD host {}", host);
//            final OpenDataDiscoveryIngestionApi client = new OpenDataDiscoveryIngestionApi(new ApiClient()
//                .setBasePath(host));
//            final Mono<ResponseEntity<Void>> res = client.postDataEntityListWithHttpInfo(dataEntityList);
//            log.info("POST - {}", res.blockOptional()
//                .map(ResponseEntity::getStatusCode)
//                .map(HttpStatus::getReasonPhrase)
//                .orElse(""));
//        } else {
//            log.warn("No ODD host configured");
//        }
    }

    @Override
    public void onJobEnd(final SparkListenerJobEnd jobEnd) {
        log.info("test#{}", jobEnd);
//        final JobResult jobResult = jobEnd.jobResult();
//        if (dataEntity != null) {
//            final DataTransformerRun dataTransformerRun = dataEntity.getDataTransformerRun();
//            if (jobResult instanceof JobFailed) {
//                dataTransformerRun.setStatus(JobRunStatus.ABORTED);
//                dataTransformerRun
//                    .statusReason(Optional
//                        .ofNullable(((JobFailed) jobResult).exception())
//                        .map(Throwable::getMessage).orElse(""));
//            } else {
//                if (dataTransformerRun.getStatus() == null) {
//                    dataTransformerRun.setStatus(JobRunStatus.SUCCESS);
//                }
//            }
//            dataTransformerRun.setEndTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(jobEnd.time()), UTC));
//        }
    }

    @Override
    public void onJobStart(final SparkListenerJobStart jobStartEvent) {
        final Optional<Long> executionId = Optional
            .ofNullable(jobStartEvent.properties().getProperty(SQLExecution.EXECUTION_ID_KEY()))
            .map(Long::parseLong);

        if (executionId.isPresent()) {
            log.debug("Current job is a SparkSQL based. Execution ID: {}", executionId.get());
//            sparkSQLExecStart(executionId.get());
        } else {
            SparkUtils.getActiveJob(jobStartEvent.jobId())
                .ifPresent(this::sparkRDDExecStart);
        }
    }

    @Override
    public void onOtherEvent(final SparkListenerEvent event) {
        if (event instanceof SparkListenerSQLExecutionStart) {
            final SparkListenerSQLExecutionStart startEvent = (SparkListenerSQLExecutionStart) event;
            log.info("sparkSQLExecStart");
            sparkSQLExecStart(startEvent.executionId());
        } else if (event instanceof SparkListenerSQLExecutionEnd) {
            sparkSQLExecEnd((SparkListenerSQLExecutionEnd) event);
        }
    }

    private void sparkRDDExecStart(final ActiveJob job) {
        log.info("Active Job Properties: {}", job.properties());
        log.info("Active Job Final RDD: {}", job.finalStage().rdd());

        final RDD<?> finalRDD = job.finalStage().rdd();
        final RddMapper rddMapper = new RddMapper();
        final String jobSuffix = rddMapper.name(finalRDD);
        final List<URI> rddInputs = rddMapper.inputs(finalRDD);
        this.inputs.addAll(DataEntityMapper.map(rddInputs)
            .stream()
            .map(DataEntity::getOddrn).collect(Collectors.toList()));
        final List<URI> rddOutputs = rddMapper.outputs(job, getConfigForRDD(finalRDD));

        this.outputs.addAll(DataEntityMapper.map(rddOutputs)
            .stream()
            .map(DataEntity::getOddrn).collect(Collectors.toList()));

        log.info("RDD jobId={} jobSuffix={} rddInputs={} rddOutputs={}", job.jobId(), jobSuffix, rddInputs, rddOutputs);
    }

    private void sparkSQLExecStart(final long executionId) {
        final QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);

        if (queryExecution != null) {
            final VisitorFactory visitorFactory = VisitorFactoryProvider.getInstance(SparkContext.getOrCreate());
            final LogicalPlan logicalPlan = queryExecution.logical();

            log.info("Found logical plan is: {}", logicalPlan);

            final SparkContext sparkContext = queryExecution.sparkPlan().sparkContext();
            final List<String> inputs = apply(visitorFactory.getVisitors(sparkContext), logicalPlan);

            log.info("INPUTS: {}", inputs);

            this.inputs.addAll(inputs);
        } else {
            log.info("OPA NET QE");
        }
    }

    private List<String> apply(final List<QueryPlanVisitor<? extends LogicalPlan, String>> visitors,
                               final LogicalPlan logicalPlan) {
        final List<String> result = new ArrayList<>();
        for (final QueryPlanVisitor<? extends LogicalPlan, String> visitor : visitors) {
            if (visitor.isDefinedAt(logicalPlan)) {
                result.addAll(visitor.apply(logicalPlan));
            }
        }

        return result;
    }

    /**
     * called by the SparkListener when a spark-sql (Dataset api) execution ends.
     */
    private void sparkSQLExecEnd(final SparkListenerSQLExecutionEnd endEvent) {
        log.info("sparkSQLExecEnd {}", endEvent);
    }
}