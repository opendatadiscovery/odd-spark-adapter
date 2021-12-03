package com.provectus.odd.adapters.spark;

import com.provectus.odd.adapters.spark.mapper.DataEntityMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkEnv$;

import org.apache.spark.scheduler.*;

import org.apache.spark.sql.SQLContext;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.opendatadiscovery.client.api.OpenDataDiscoveryIngestionApi;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityList;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Mono;
import scala.Option;

import static com.provectus.odd.adapters.spark.utils.ScalaConversionUtils.findSparkConfigKey;
import static com.provectus.odd.adapters.spark.utils.Utils.sanitizeJdbcUrl;


@Slf4j
public class OddAdapterSparkListener extends SparkListener {
    public static final String ODD_HOST_CONFIG_KEY = "odd.host.url";

    private OpenDataDiscoveryIngestionApi client;

    private final DataEntityMapper dataEntityMapper = DataEntityMapper.INSTANCE;

    private int jobCount = 0;

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        log.info("onApplicationStart: {}", applicationStart);
        SparkEnv sparkEnv = SparkEnv$.MODULE$.get();
        SparkConf conf = sparkEnv.conf();
        String endpoint = findSparkConfigKey(conf, ODD_HOST_CONFIG_KEY, null);
        if (endpoint != null) {
            log.info("Setting {} ODD host", endpoint);
            client = new OpenDataDiscoveryIngestionApi();
            client.getApiClient().setBasePath(endpoint);
        } else {
            log.warn("No ODD host configured");
        }
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        log.info("onApplicationEnd: {} jobsCount {}", applicationEnd, jobCount);
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        JobResult jobResult = jobEnd.jobResult();
        String status;
        if (jobResult instanceof JobFailed) {
            status = "FAIL";
        } else {
            status = "SUCCESS";
        }
        log.info("onJobEnd {} status {}", jobEnd, status);
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        jobCount += 1;
        log.info("onJobStart#{} {}", jobCount, jobStart.properties());
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        if (event instanceof SparkListenerSQLExecutionStart) {
            DataEntityList dataEntityList = sparkSQLExecStart((SparkListenerSQLExecutionStart) event);
            Mono<ResponseEntity<Void>> res = client.postDataEntityListWithHttpInfo(dataEntityList);
            log.info("Response {}", res.blockOptional()
                    .map(ResponseEntity::getStatusCode)
                    .map(HttpStatus::getReasonPhrase)
                    .orElse("<EMPTY>"));
        } else if (event instanceof SparkListenerSQLExecutionEnd) {
            sparkSQLExecEnd((SparkListenerSQLExecutionEnd) event);
        }
    }

    /**
     * called by the SparkListener when a spark-sql (Dataset api) execution starts
     */
    private DataEntityList sparkSQLExecStart(SparkListenerSQLExecutionStart startEvent) {
        log.info("sparkSQLExecStart {}", startEvent);
        QueryExecution queryExecution = SQLExecution.getQueryExecution(startEvent.executionId());

        SparkPlan sparkPlan = queryExecution.sparkPlan();
        //log.info("sparkPlan {}", sparkPlan.prettyJson());
        LogicalPlan logicalPlan = queryExecution.logical();

        if (logicalPlan instanceof SaveIntoDataSourceCommand) {
            return handleSaveIntoDataSourceCommand(logicalPlan, sparkPlan.sqlContext());
        }
        LogicalRelation logRel = findLogicalRelation(logicalPlan);

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

    private DataEntityList handleSaveIntoDataSourceCommand(LogicalPlan logicalPlan, SQLContext sqlContext) {
        SaveIntoDataSourceCommand command = (SaveIntoDataSourceCommand) logicalPlan;
        BaseRelation relation;
        if (command.dataSource() instanceof RelationProvider) {
            RelationProvider p = (RelationProvider) command.dataSource();
            relation = p.createRelation(sqlContext, command.options());
        } else {
            SchemaRelationProvider p = (SchemaRelationProvider) command.dataSource();
            relation = p.createRelation(sqlContext, command.options(), logicalPlan.schema());
        }
        LogicalRelation logicalRelation = new LogicalRelation(relation, relation.schema().toAttributes(), Option.empty(),
                logicalPlan.isStreaming());

        DataEntity dataEntity = dataEntityMapper.map(logicalRelation)
                .name(command.options().get("dbtable").get());
        DataEntityList dataEntityList = new DataEntityList()
                //TODO replace w oddrn
                .dataSourceOddrn(sanitizeJdbcUrl(command.options().get("url").get()))
                .addItemsItem(dataEntity);
        log.info("{}", dataEntityList);
        return dataEntityList;
    }

    private DataEntityList handleHadoopFsRelation(LogicalRelation logicalRelation) {
        log.info("TODO: handleHadoopFsRelation");
        return new DataEntityList();
    }

    private DataEntityList handleCatalogTable(LogicalRelation logicalRelation) {
        log.info("TODO: handleCatalogTable");
        return new DataEntityList();
    }

    private DataEntityList handleJdbcRelation(LogicalRelation logicalRelation) {
        var relation = (JDBCRelation) logicalRelation.relation();
        DataEntity dataEntity = dataEntityMapper.map(relation);
        DataEntityList dataEntityList = new DataEntityList()
                //TODO replace w oddrn
                .dataSourceOddrn(sanitizeJdbcUrl(relation.jdbcOptions().url()))
                .addItemsItem(dataEntity);
        log.info("{}", dataEntityList);
        return dataEntityList;
    }
}
