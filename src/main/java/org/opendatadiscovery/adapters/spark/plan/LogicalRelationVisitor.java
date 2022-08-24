package org.opendatadiscovery.adapters.spark.plan;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.opendatadiscovery.adapters.spark.mapper.DataEntityMapper;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.client.model.DataEntity;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class LogicalRelationVisitor extends QueryPlanVisitor<LogicalRelation, DataEntity> {
    private final SparkContext context;

    public LogicalRelationVisitor(final SparkContext context) {
        this.context = context;
    }

    @Override
    public boolean isDefinedAt(final LogicalPlan logicalPlan) {
        return logicalPlan instanceof LogicalRelation || logicalPlan instanceof UnaryNode;
    }

    @Override
    public List<DataEntity> apply(final LogicalPlan logicalPlan) {
        final Optional<LogicalRelation> logRelOpt = findLogicalRelation(logicalPlan);

        if (!logRelOpt.isPresent()) {
            return new ArrayList<>();
        }

        final LogicalRelation logRel = logRelOpt.get();

        if (logRel.relation() instanceof HadoopFsRelation) {
            return handleDelta((HadoopFsRelation) logRel.relation());
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

    private Optional<LogicalRelation> findLogicalRelation(final LogicalPlan logicalPlan) {
        if (logicalPlan instanceof LogicalRDD) {
            return Optional.empty();
        }

        if (logicalPlan instanceof LogicalRelation) {
            return Optional.of((LogicalRelation) logicalPlan);
        }

        return findLogicalRelation(((UnaryNode) logicalPlan).child());
    }

    private List<DataEntity> handleCatalogTable(final LogicalRelation logRel) {
        final CatalogTable catalogTable = logRel.catalogTable().get();
        return Collections.singletonList(new DataEntity()
            .oddrn(catalogTable.location().getPath())
        );
    }

    private List<DataEntity> handleDelta(final HadoopFsRelation relation) {
        return JavaConversions.asJavaCollection(relation.location().rootPaths()).stream()
            .map(p -> Utils.getDirectoryPath(p, context.hadoopConfiguration()))
            .distinct()
            .map(path -> DataEntityMapper.map(Utils.namespaceUri(path.toUri()), path.toString()))
            .collect(Collectors.toList());
    }

    private List<DataEntity> handleHadoopFsRelation(final HadoopFsRelation relation) {
        return JavaConversions.asJavaCollection(relation.location().rootPaths()).stream()
            .map(p -> Utils.getDirectoryPath(p, context.hadoopConfiguration()))
            .distinct()
            .map(
                path -> {
                    final String namespace = Utils.namespaceUri(path.toUri());
                    final String file = Arrays.stream(relation.location().inputFiles())
                        .filter(f -> f.contains(namespace))
                        .collect(Collectors.joining());

                    return DataEntityMapper.map(namespace, file);
                })
            .collect(Collectors.toList());
    }

    private List<DataEntity> handleJdbcRelation(final JDBCRelation relation) {
        final String url = relation.jdbcOptions().url();
        final String tableOrQuery = relation.jdbcOptions().tableOrQuery();
        final List<String> tables = extractTableNames(tableOrQuery);
        if (tables.isEmpty()) {
            return Collections.singletonList(DataEntityMapper.map(null, url, tableOrQuery));
        }
        return tables.stream().map(tableName -> DataEntityMapper.map(tableOrQuery, url, tableName))
            .collect(Collectors.toList());
    }

    private List<String> extractTableNames(final String tableOrQuery) {
        try {
            final String sql = tableOrQuery.substring(tableOrQuery.indexOf("(") + 1, tableOrQuery.indexOf(")"));
            final Statement statement = CCJSqlParserUtil.parse(sql);
            return new TablesNamesFinder().getTableList(statement);
        } catch (StringIndexOutOfBoundsException ignored) {
            log.info("JdbcRelation table found: {}", tableOrQuery);
        } catch (JSQLParserException e) {
            log.warn("JDBCRelation ", e);
        }
        return Collections.emptyList();
    }
}
