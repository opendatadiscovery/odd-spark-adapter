package com.provectus.odd.adapters.spark;

import com.provectus.odd.adapters.spark.mapper.DataEntityMapper;
import com.provectus.odd.adapters.spark.mapper.DataTransformerMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkEnv$;


import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerEvent;

import org.apache.spark.sql.SQLContext;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.opendatadiscovery.client.api.OpenDataDiscoveryIngestionApi;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityList;
import org.opendatadiscovery.client.model.DataTransformer;
import org.opendatadiscovery.client.model.DataTransformerRun;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;

import static com.provectus.odd.adapters.spark.utils.ScalaConversionUtils.findSparkConfigKey;
import static java.time.ZoneOffset.UTC;


@Slf4j
public class OddAdapterSparkListener extends SparkListener {
    public static final String ODD_HOST_CONFIG_KEY = "odd.host.url";

    private OpenDataDiscoveryIngestionApi client;

    private final DataTransformerMapper dataTransformerMapper = new DataTransformerMapper();

    private final DataEntityMapper dataEntityMapper = new DataEntityMapper();

    private final DataTransformer dataTransformerAcc = new DataTransformer();

    private DataEntity dataEntity = null;

    private int jobCount = 0;

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        log.info("onApplicationStart: {}", applicationStart);
        var sparkEnv = SparkEnv$.MODULE$.get();
        var conf = sparkEnv.conf();
        var endpoint = findSparkConfigKey(conf, ODD_HOST_CONFIG_KEY, null);
        if (endpoint != null) {
            log.info("Setting ODD host {}", endpoint);
            client = new OpenDataDiscoveryIngestionApi();
            client.getApiClient().setBasePath(endpoint);
        } else {
            log.warn("No ODD host configured");
        }
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        log.info("onApplicationEnd: {} jobsCount {}", applicationEnd, jobCount);
        var dataEntityList = dataEntityList();
        log.info("{}", dataEntityList);
        var res = client.postDataEntityListWithHttpInfo(dataEntityList);
        log.info("Response {}", res.blockOptional()
                .map(ResponseEntity::getStatusCode)
                .map(HttpStatus::getReasonPhrase)
                .orElse("<EMPTY>"));
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        var jobResult = jobEnd.jobResult();
        var dataTransformerRun = dataEntity.getDataTransformerRun();
        if (jobResult instanceof JobFailed) {
            dataTransformerRun.setStatus(DataTransformerRun.StatusEnum.FAILED);
            dataTransformerRun
                    .statusReason(Optional
                            .ofNullable(((JobFailed)jobResult).exception())
                            .map(Throwable::getMessage).orElse(""));
        } else {
            if (dataTransformerRun.getStatus() == null) {
                dataTransformerRun.setStatus(DataTransformerRun.StatusEnum.SUCCESS);
            }
        }
        dataTransformerRun.setEndTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(jobEnd.time()), UTC));
        log.info("onJobEnd {}", jobEnd);
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        jobCount += 1;
        log.info("onJobStart#{} {}", jobCount, jobStart.properties());
        if (dataEntity == null) {
            dataEntity = dataEntityMapper.map(jobStart);
        }
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        if (event instanceof SparkListenerSQLExecutionStart) {
            var dataTransformer = sparkSQLExecStart((SparkListenerSQLExecutionStart) event);
            if (dataTransformer.getInputs().isEmpty()) {
                dataTransformerAcc.getOutputs().addAll(dataTransformer.getOutputs());
            } else {
                dataTransformerAcc.getInputs().addAll(dataTransformer.getInputs());
            }
        } else if (event instanceof SparkListenerSQLExecutionEnd) {
            sparkSQLExecEnd((SparkListenerSQLExecutionEnd) event);
        }
    }

    private DataEntityList dataEntityList() {
        return new DataEntityList()
                .dataSourceOddrn(dataEntity.getOddrn())
                .addItemsItem(dataEntityMapper.map(dataEntity).dataTransformer(dataTransformerAcc))
                .addItemsItem(dataEntity);
    }

    /**
     * called by the SparkListener when a spark-sql (Dataset api) execution starts
     */
    private DataTransformer sparkSQLExecStart(SparkListenerSQLExecutionStart startEvent) {
        log.info("sparkSQLExecStart {}", startEvent);
        var queryExecution = SQLExecution.getQueryExecution(startEvent.executionId());

        var sparkPlan = queryExecution.sparkPlan();
        //log.info("sparkPlan {}", sparkPlan.prettyJson());
        var logicalPlan = queryExecution.logical();

        if (logicalPlan instanceof SaveIntoDataSourceCommand) {
            return handleSaveIntoDataSourceCommand(logicalPlan, sparkPlan.sqlContext());
        }
        var logRel = findLogicalRelation(logicalPlan);

        if (logRel.relation() instanceof JDBCRelation) {
            return handleJdbcRelation(logRel);
        } else if (logRel.relation() instanceof HadoopFsRelation) {
            return handleHadoopFsRelation(logRel);
        } else if (logRel.catalogTable().isDefined()) {
            return handleCatalogTable(logRel);
        }
        throw new IllegalArgumentException(
                "Expected logical plan to be either HadoopFsRelation, JDBCRelation, or CatalogTable but was "
                        + logicalPlan);
    }

    private LogicalRelation findLogicalRelation(LogicalPlan logicalPlan) {
        if (logicalPlan instanceof LogicalRelation) {
            return (LogicalRelation) logicalPlan;
        }
        return findLogicalRelation(((UnaryNode) logicalPlan).child());
    }

    /**
     * called by the SparkListener when a spark-sql (Dataset api) execution ends
     */
    private void sparkSQLExecEnd(SparkListenerSQLExecutionEnd endEvent) {
        log.info("sparkSQLExecEnd {}", endEvent);
    }

    private DataTransformer handleSaveIntoDataSourceCommand(LogicalPlan logicalPlan, SQLContext sqlContext) {
        var command = (SaveIntoDataSourceCommand) logicalPlan;
        return dataTransformerMapper.map(command);
    }

    private DataTransformer handleHadoopFsRelation(LogicalRelation logicalRelation) {
        log.info("TODO: handleHadoopFsRelation");
        return new DataTransformer();
    }

    private DataTransformer handleCatalogTable(LogicalRelation logicalRelation) {
        log.info("TODO: handleCatalogTable");
        return new DataTransformer();
    }

    private DataTransformer handleJdbcRelation(LogicalRelation logicalRelation) {
        var relation = (JDBCRelation) logicalRelation.relation();
        return dataTransformerMapper.map(relation);
    }
}
