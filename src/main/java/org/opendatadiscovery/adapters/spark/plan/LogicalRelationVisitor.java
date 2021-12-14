package org.opendatadiscovery.adapters.spark.plan;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.opendatadiscovery.adapters.spark.mapper.DataEntityMapper;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityType;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.opendatadiscovery.adapters.spark.utils.Utils.fileGenerator;
import static org.opendatadiscovery.adapters.spark.utils.Utils.s3Generator;
import static org.opendatadiscovery.adapters.spark.utils.Utils.S3A;


@Slf4j
public class LogicalRelationVisitor extends QueryPlanVisitor<LogicalRelation, DataEntity> {
    private final SparkContext context;

    public LogicalRelationVisitor(SparkContext context) {
        this.context = context;
    }

    @Override
    public boolean isDefinedAt(LogicalPlan logicalPlan) {
        return logicalPlan instanceof LogicalRelation || logicalPlan instanceof UnaryNode;
    }

    @Override
    public List<DataEntity> apply(LogicalPlan logicalPlan) {
        LogicalRelation logRel = findLogicalRelation(logicalPlan);
        if (logRel.relation() instanceof HadoopFsRelation) {
            return handleHadoopFsRelation((HadoopFsRelation) logRel.relation());
        } else if (logRel.relation() instanceof JDBCRelation) {
            return handleJdbcRelation((JDBCRelation) logRel.relation());
        } else if (logRel.catalogTable().isDefined()) {
            return handleCatalogTable(logRel);
        }
        throw new IllegalArgumentException(
                "Expected logical plan to be either HadoopFsRelation, JDBCRelation, "
                        + "or CatalogTable but was "
                        + logicalPlan);
    }

    private LogicalRelation findLogicalRelation(LogicalPlan logicalPlan) {
        if (logicalPlan instanceof LogicalRelation) {
            return (LogicalRelation) logicalPlan;
        }
        return findLogicalRelation(((UnaryNode) logicalPlan).child());
    }

    private List<DataEntity> handleCatalogTable(LogicalRelation logRel) {
        CatalogTable catalogTable = logRel.catalogTable().get();
        return Collections.singletonList(new DataEntity()
                .oddrn(catalogTable.location().getPath())
        );
    }

    private List<DataEntity> handleHadoopFsRelation(HadoopFsRelation relation) {
        return JavaConversions.asJavaCollection(relation.location().rootPaths()).stream()
                .map(p -> Utils.getDirectoryPath(p, context.hadoopConfiguration()))
                .distinct()
                .map(
                        path -> {
                            var namespace = Utils.namespaceUri(path.toUri());
                            var file = Arrays.stream(relation.location().inputFiles())
                                    .filter(f -> f.contains(namespace))
                                    .collect(Collectors.joining());
                            if (namespace.contains(S3A)) {
                                return new DataEntity()
                                        .type(DataEntityType.FILE)
                                        .oddrn(s3Generator(namespace, file));
                            }
                            return new DataEntity()
                                    .type(DataEntityType.FILE)
                                    .oddrn(fileGenerator(namespace, file));
                        })
                .collect(Collectors.toList());
    }

    private List<DataEntity> handleJdbcRelation(JDBCRelation relation) {

        var url = relation.jdbcOptions().url();
        var tableOrQuery = relation.jdbcOptions().tableOrQuery();
        var tables = extractTableNames(tableOrQuery);
        if (tables.isEmpty()) {
            return Collections.singletonList(DataEntityMapper.map(null, url, tableOrQuery));
        }
        return tables.stream().map(tableName -> DataEntityMapper.map(tableOrQuery, url, tableName)).collect(Collectors.toList());
    }

    private List<String> extractTableNames(String tableOrQuery) {
        try {
            var sql = tableOrQuery.substring(tableOrQuery.indexOf("(") + 1, tableOrQuery.indexOf(")"));
            var statement = CCJSqlParserUtil.parse(sql);
            return new TablesNamesFinder().getTableList(statement);
        } catch (StringIndexOutOfBoundsException ignored) {
            log.info("JdbcRelation table found: {}", tableOrQuery);
        } catch (JSQLParserException e) {
            log.warn("JDBCRelation ", e);
        }
        return Collections.emptyList();
    }
}
