package org.opendatadiscovery.adapters.spark;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.WeakHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.SparkEnv$;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobResult;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.opendatadiscovery.adapters.spark.mapper.DataEntityMapper;
import org.opendatadiscovery.adapters.spark.mapper.RddMapper;
import org.opendatadiscovery.adapters.spark.plan.QueryPlanVisitor;
import org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.client.ApiClient;
import org.opendatadiscovery.client.api.OpenDataDiscoveryIngestionApi;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityList;
import org.opendatadiscovery.client.model.DataTransformerRun;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import static java.time.ZoneOffset.UTC;

@Slf4j
public class OddAdapterSparkListener extends SparkListener {
    public static final String ODD_HOST_CONFIG_KEY = "odd.host.url";
    public static final String SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id";

    private final List<DataEntity> inputs = Collections.synchronizedList(new ArrayList<>());

    private final List<DataEntity> outputs = Collections.synchronizedList(new ArrayList<>());

    private DataEntity dataEntity = null;

    private int jobCount = 0;

    private static final Properties PROPERTIES = new Properties();

    private static final WeakHashMap<RDD<?>, Configuration> RDD_CONFIG = new WeakHashMap<>();

    @SuppressWarnings("unused")
    public static void instrument(final SparkContext context) {
        log.info(
                "Initialized ODD listener with \nspark version: {}\njava.version: {}\nconfiguration: {}",
                context.version(),
                System.getProperty("java.version"),
                context.conf().toDebugString());
        final OddAdapterSparkListener listener = new OddAdapterSparkListener();
        listener.dataEntity = DataEntityMapper.map(context);
        context.addSparkListener(listener);
    }

    @SuppressWarnings("unused")
    public static void registerOutput(final PairRDDFunctions<?, ?> pairRDDFunctions, final Configuration conf) {
        try {
            log.info("Initializing ODD PairRDDFunctions listener...");
            final Field[] declaredFields = pairRDDFunctions.getClass().getDeclaredFields();
            for (final Field field : declaredFields) {
                if (field.getName().endsWith("self") && RDD.class.isAssignableFrom(field.getType())) {
                    field.setAccessible(true);
                    try {
                        final RDD<?> rdd = (RDD<?>) field.get(pairRDDFunctions);
                        RDD_CONFIG.put(rdd, conf);
                    } catch (IllegalArgumentException | IllegalAccessException e) {
                        e.printStackTrace(System.out);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Could not initialize ODD PairRDDFunctions listener", e);
        }
    }

    public static Configuration getConfigForRDD(final RDD<?> rdd) {
        return RDD_CONFIG.get(rdd);
    }

    public static void setProperties(final String agentArgs) {
        PROPERTIES.setProperty(ODD_HOST_CONFIG_KEY, agentArgs);
    }

    @Override
    public void onApplicationStart(final SparkListenerApplicationStart applicationStart) {
        log.info("onApplicationStart: {}", applicationStart);
    }

    @Override
    public void onApplicationEnd(final SparkListenerApplicationEnd applicationEnd) {
        log.info("onApplicationEnd: {} jobsCount {}", applicationEnd, jobCount);
        RDD_CONFIG.clear();
        final DataEntityList dataEntityList = DataEntityMapper.map(dataEntity, inputs, outputs);
        log.info("{}", dataEntityList);
        final SparkConf conf = SparkEnv$.MODULE$.get().conf();
        final String host = ScalaConversionUtils.findSparkConfigKey(conf, ODD_HOST_CONFIG_KEY)
                .map(x -> PROPERTIES.getProperty(ODD_HOST_CONFIG_KEY))
                .orElse(Utils.getProperty(System.getProperties(), ODD_HOST_CONFIG_KEY));
        if (host != null) {
            log.info("Setting ODD host {}", host);
            final OpenDataDiscoveryIngestionApi client = new OpenDataDiscoveryIngestionApi(new ApiClient()
                    .setBasePath(host));
            final Mono<ResponseEntity<Void>> res = client.postDataEntityListWithHttpInfo(dataEntityList);
            log.info("POST - {}", res.blockOptional()
                    .map(ResponseEntity::getStatusCode)
                    .map(HttpStatus::getReasonPhrase)
                    .orElse(""));
        } else {
            log.warn("No ODD host configured");
        }
    }

    @Override
    public void onJobEnd(final SparkListenerJobEnd jobEnd) {
        log.info("onJobEnd {}", jobEnd);
        final JobResult jobResult = jobEnd.jobResult();
        if (dataEntity != null) {
            final DataTransformerRun dataTransformerRun = dataEntity.getDataTransformerRun();
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
    public void onJobStart(final SparkListenerJobStart jobStart) {
        jobCount += 1;
        final Properties properties = jobStart.properties();
        log.info("onJobStart#{} {}", jobCount, properties);
        if (dataEntity == null) {
            if (StringUtils.hasText(properties.getProperty("spark.rdd.scope"))) {
                dataEntity = DataEntityMapper.map(SparkContext.getOrCreate());
            } else {
                dataEntity = DataEntityMapper.map(properties);
            }
        } else {
            final Map props = jobStart.properties();
            dataEntity.getMetadata().get(0).getMetadata().putAll((Map<String, Object>) props);
        }
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
                            final String executionIdProp = job.properties().getProperty(SPARK_SQL_EXECUTION_ID);
                            if (executionIdProp != null) {
                                final long executionId = Long.parseLong(executionIdProp);
                                log.info("{}: {}", SPARK_SQL_EXECUTION_ID, executionId);
                            } else {
                                sparkRDDExecStart(job);
                            }
                        });
    }

    @Override
    public void onOtherEvent(final SparkListenerEvent event) {
        if (event instanceof SparkListenerSQLExecutionStart) {
            final SparkListenerSQLExecutionStart startEvent = (SparkListenerSQLExecutionStart) event;
            log.info("sparkSQLExecStart {}", startEvent);
            sparkSQLExecStart(startEvent.executionId());
        } else if (event instanceof SparkListenerSQLExecutionEnd) {
            sparkSQLExecEnd((SparkListenerSQLExecutionEnd) event);
        }
    }

    private void sparkRDDExecStart(final ActiveJob job) {
        final RDD<?> finalRDD = job.finalStage().rdd();
        final RddMapper rddMapper = new RddMapper();
        final String jobSuffix = rddMapper.name(finalRDD);
        final List<URI> rddInputs = rddMapper.inputs(finalRDD);
        this.inputs.addAll(DataEntityMapper.map(rddInputs));
        final List<URI> rddOutputs = rddMapper.outputs(job,
                OddAdapterSparkListener.getConfigForRDD(finalRDD));
        this.outputs.addAll(DataEntityMapper.map(rddOutputs));
        log.info("RDD jobId={} jobSuffix={} rddInputs={} rddOutputs={}",
                job.jobId(), jobSuffix, rddInputs, rddOutputs);
    }

    /**
     * called by the SparkListener when a spark-sql (Dataset api) execution starts.
     */
    private void sparkSQLExecStart(final long executionId) {
        final QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);
        if (queryExecution != null) {
            final VisitorFactory visitorFactory = VisitorFactoryProvider.getInstance(SparkContext.getOrCreate());
            final LogicalPlan logicalPlan = queryExecution.logical();
            final SQLContext sqlContext = queryExecution.sparkPlan().sqlContext();
            final List<DataEntity> inputs = apply(visitorFactory.getInputVisitors(sqlContext), logicalPlan);
            if (inputs.isEmpty()) {
                final List<DataEntity> outputs = apply(visitorFactory.getOutputVisitors(sqlContext), logicalPlan);
                this.outputs.addAll(outputs);
            } else {
                this.inputs.addAll(inputs);
            }
        }
    }

    private List<DataEntity> apply(final List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> visitors,
                                   final LogicalPlan logicalPlan) {
        return visitors.stream()
                .filter(v -> v.isDefinedAt(logicalPlan))
                .map(v -> v.apply(logicalPlan))
                .findFirst()
                .orElse(Collections.emptyList());
    }

    /**
     * called by the SparkListener when a spark-sql (Dataset api) execution ends.
     */
    private void sparkSQLExecEnd(final SparkListenerSQLExecutionEnd endEvent) {
        log.info("sparkSQLExecEnd {}", endEvent);
    }
}
