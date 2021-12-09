package org.opendatadiscovery.adapters.spark;

import org.opendatadiscovery.adapters.spark.mapper.DataEntityMapper;
import org.opendatadiscovery.adapters.spark.mapper.RddMapper;
import org.opendatadiscovery.adapters.spark.plan.QueryPlanVisitor;
import org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.SparkEnv$;


import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerEvent;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.opendatadiscovery.client.ApiClient;
import org.opendatadiscovery.client.api.OpenDataDiscoveryIngestionApi;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataTransformerRun;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.WeakHashMap;

import static org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils.findSparkConfigKey;
import static java.time.ZoneOffset.UTC;


@Slf4j
public class OddAdapterSparkListener extends SparkListener {
    public static final String ODD_HOST_CONFIG_KEY = "odd.host.url";
    public static final String SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id";

    private final List<DataEntity> inputs = Collections.synchronizedList(new ArrayList<>());

    private final List<DataEntity> outputs = Collections.synchronizedList(new ArrayList<>());

    private DataEntity dataEntity = null;

    private int jobCount = 0;

    private static final Properties properties = new Properties();

    private static final WeakHashMap<RDD<?>, Configuration> rddConfig = new WeakHashMap<>();

    @SuppressWarnings("unused")
    public static void instrument(SparkContext context) {
        log.info(
                "Initialized ODD listener with \nspark version: {}\njava.version: {}\nconfiguration: {}",
                context.version(),
                System.getProperty("java.version"),
                context.conf().toDebugString());
        OddAdapterSparkListener listener = new OddAdapterSparkListener();
        context.addSparkListener(listener);
    }

    @SuppressWarnings("unused")
    public static void registerOutput(PairRDDFunctions<?, ?> pairRDDFunctions, Configuration conf) {
        try {
            log.info("Initializing ODD PairRDDFunctions listener...");
            Field[] declaredFields = pairRDDFunctions.getClass().getDeclaredFields();
            for (Field field : declaredFields) {
                if (field.getName().endsWith("self") && RDD.class.isAssignableFrom(field.getType())) {
                    field.setAccessible(true);
                    try {
                        RDD<?> rdd = (RDD<?>) field.get(pairRDDFunctions);
                        rddConfig.put(rdd, conf);
                    } catch (IllegalArgumentException | IllegalAccessException e) {
                        e.printStackTrace(System.out);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Could not initialize ODD PairRDDFunctions listener", e);
        }
    }

    public static Configuration getConfigForRDD(RDD<?> rdd) {
        return rddConfig.get(rdd);
    }

    public static void setProperties(String agentArgs) {
        properties.setProperty(ODD_HOST_CONFIG_KEY, agentArgs);
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        log.info("onApplicationStart: {}", applicationStart);
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        log.info("onApplicationEnd: {} jobsCount {}", applicationEnd, jobCount);
        rddConfig.clear();
        var dataEntityList = DataEntityMapper.map(dataEntity, inputs, outputs);
        log.info("{}", dataEntityList);
        var conf = SparkEnv$.MODULE$.get().conf();
        var host = findSparkConfigKey(conf, ODD_HOST_CONFIG_KEY,
                properties.getProperty(ODD_HOST_CONFIG_KEY));
        if (host != null) {
            log.info("Setting ODD host {}", host);
            var client = new OpenDataDiscoveryIngestionApi(new ApiClient().setBasePath(host));
            var res = client.postDataEntityListWithHttpInfo(dataEntityList);
            log.info("POST - {}", res.blockOptional()
                    .map(ResponseEntity::getStatusCode)
                    .map(HttpStatus::getReasonPhrase)
                    .orElse(""));
        } else {
            log.warn("No ODD host configured");
        }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        log.info("onJobEnd {}", jobEnd);
        var jobResult = jobEnd.jobResult();
        if (dataEntity != null) {
            var dataTransformerRun = dataEntity.getDataTransformerRun();
            if (jobResult instanceof JobFailed) {
                dataTransformerRun.setStatus(DataTransformerRun.StatusEnum.FAILED);
                dataTransformerRun
                        .statusReason(Optional
                                .ofNullable(((JobFailed) jobResult).exception())
                                .map(Throwable::getMessage).orElse(""));
            } else {
                if (dataTransformerRun.getStatus() == null) {
                    dataTransformerRun.setStatus(DataTransformerRun.StatusEnum.SUCCESS);
                }
            }
            dataTransformerRun.setEndTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(jobEnd.time()), UTC));
        }
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        jobCount += 1;
        log.info("onJobStart#{} {}", jobCount, jobStart.properties());
        ScalaConversionUtils.asJavaOptional(
                SparkSession.getActiveSession()
                        .map(ScalaConversionUtils.toScalaFn(SparkSession::sparkContext))
                        .orElse(ScalaConversionUtils.toScalaFn(() -> SparkContext$.MODULE$.getActive())))
                .flatMap(
                        ctx ->
                                ScalaConversionUtils.asJavaOptional(
                                        ctx.dagScheduler().jobIdToActiveJob().get(jobStart.jobId())))
                .ifPresent(
                        job -> {
                            String executionIdProp = job.properties().getProperty(SPARK_SQL_EXECUTION_ID);
                            if (executionIdProp != null) {
                                long executionId = Long.parseLong(executionIdProp);
                                log.info("{}: {}", SPARK_SQL_EXECUTION_ID, executionId);
                                if (dataEntity == null) {
                                    dataEntity = DataEntityMapper.map(jobStart);
                                }
                            } else {
                                var finalRDD = job.finalStage().rdd();
                                var rddMapper = new RddMapper();
                                var jobSuffix = rddMapper.name(finalRDD);
                                var rddInputs = rddMapper.inputs(finalRDD);
                                this.inputs.addAll(rddInputs);
                                var rddOutputs = rddMapper.outputs(job,
                                        OddAdapterSparkListener.getConfigForRDD(finalRDD));
                                this.outputs.addAll(rddOutputs);
                                log.info("RDD jobId={} jobSuffix={} rddInputs={} rddOutputs={}",
                                        job.jobId(), jobSuffix, rddInputs, rddOutputs);
                            }
                        });
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        if (event instanceof SparkListenerSQLExecutionStart) {
            var startEvent = (SparkListenerSQLExecutionStart) event;
            log.info("sparkSQLExecStart {}", startEvent);
            sparkSQLExecStart(startEvent.executionId());
        } else if (event instanceof SparkListenerSQLExecutionEnd) {
            sparkSQLExecEnd((SparkListenerSQLExecutionEnd) event);
        }
    }

    /**
     * called by the SparkListener when a spark-sql (Dataset api) execution starts
     */
    private void sparkSQLExecStart(long executionId) {
        var queryExecution = SQLExecution.getQueryExecution(executionId);
        if (queryExecution != null) {
            //log.info("sparkPlan {}", queryExecution.sparkPlan().prettyJson());
            var visitorFactory = VisitorFactoryProvider.getInstance(SparkContext.getOrCreate());
            var logicalPlan = queryExecution.logical();
            var sqlContext = queryExecution.sparkPlan().sqlContext();
            var inputs = apply(visitorFactory.getInputVisitors(sqlContext), logicalPlan);
            if (inputs.isEmpty()) {
                var outputs = apply(visitorFactory.getOutputVisitors(sqlContext), logicalPlan);
                this.outputs.addAll(outputs);
            } else {
                this.inputs.addAll(inputs);
            }
        }
    }

    private List<DataEntity> apply(List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> visitors,
                                   LogicalPlan logicalPlan) {
        return visitors.stream()
                .filter(v -> v.isDefinedAt(logicalPlan))
                .map(v -> v.apply(logicalPlan))
                .findFirst()
                .orElse(Collections.emptyList());
    }

    /**
     * called by the SparkListener when a spark-sql (Dataset api) execution ends
     */
    private void sparkSQLExecEnd(SparkListenerSQLExecutionEnd endEvent) {
        log.info("sparkSQLExecEnd {}", endEvent);
    }
}
