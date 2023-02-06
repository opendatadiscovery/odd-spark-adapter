package org.opendatadiscovery.adapters.spark.plan;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;
import org.opendatadiscovery.adapters.spark.mapper.DataEntityMapper;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.client.model.DataEntity;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class LogicalRelationVisitor extends QueryPlanVisitor<LogicalRelation> {
    private final SparkContext context;

    @Override
    public boolean isDefinedAt(final LogicalPlan logicalPlan) {
        return logicalPlan instanceof LogicalRelation
            && (((LogicalRelation) logicalPlan).relation() instanceof HadoopFsRelation
            || ((LogicalRelation) logicalPlan).relation() instanceof JDBCRelation
            || ((LogicalRelation) logicalPlan).catalogTable().isDefined());
    }

    @Override
    public LogicalPlanDependencies apply(final LogicalPlan logicalPlan) {
        final LogicalRelation logRel = findLogicalRelation(logicalPlan);
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

    private LogicalRelation findLogicalRelation(final LogicalPlan logicalPlan) {
        if (logicalPlan instanceof LogicalRelation) {
            return (LogicalRelation) logicalPlan;
        }
        return findLogicalRelation(((UnaryNode) logicalPlan).child());
    }

    private LogicalPlanDependencies handleCatalogTable(final LogicalRelation logRel) {
        final CatalogTable catalogTable = logRel.catalogTable().get();
        return LogicalPlanDependencies.inputs(Collections.singletonList(catalogTable.location().getPath()));
    }

    private LogicalPlanDependencies handleHadoopFsRelation(final HadoopFsRelation relation) {
        final List<String> inputs = JavaConverters.asJavaCollection(relation.location().rootPaths())
            .stream()
            .map(p -> Utils.getDirectoryPath(p, context.hadoopConfiguration()))
            .distinct()
            .map(path -> path.toUri().toString())
            .collect(Collectors.toList());

        return LogicalPlanDependencies.inputs(inputs);
//            .map(path -> {
//                    final String namespace = Utils.namespaceUri(path.toUri());
//                    final String file = Arrays.stream(relation.location().inputFiles())
//                        .filter(f -> f.contains(namespace))
//                        .collect(Collectors.joining());
//                    return DataEntityMapper.map(namespace, file);
//            })
//            .collect(Collectors.toList());
    }

    private LogicalPlanDependencies handleJdbcRelation(final JDBCRelation relation) {
        final String url = relation.jdbcOptions().url();
        final String tableOrQuery = relation.jdbcOptions().tableOrQuery();
        final List<String> tables = extractTableNames(tableOrQuery).getInputs();

        final List<String> inputs;
        if (tables.isEmpty()) {
            inputs = Collections.singletonList(DataEntityMapper.map(null, url, tableOrQuery))
                .stream()
                .map(DataEntity::getOddrn)
                .collect(Collectors.toList());
        } else {
            inputs = tables.stream().map(tableName -> DataEntityMapper.map(tableOrQuery, url, tableName))
                .map(DataEntity::getOddrn)
                .collect(Collectors.toList());
        }
        return LogicalPlanDependencies.inputs(inputs);
    }

    private LogicalPlanDependencies extractTableNames(final String tableOrQuery) {
        try {
            final String sql = tableOrQuery.substring(tableOrQuery.indexOf("(") + 1, tableOrQuery.indexOf(")"));
            final Statement statement = CCJSqlParserUtil.parse(sql);
            return LogicalPlanDependencies.inputs(new TablesNamesFinder().getTableList(statement));
        } catch (StringIndexOutOfBoundsException ignored) {
            log.info("JdbcRelation table found: {}", tableOrQuery);
        } catch (JSQLParserException e) {
            log.warn("JDBCRelation ", e);
        }

        return LogicalPlanDependencies.empty();
    }
}
