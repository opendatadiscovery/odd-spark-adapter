package org.opendatadiscovery.adapters.spark.visitor.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;
import org.opendatadiscovery.adapters.spark.utils.OddrnUtils;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.adapters.spark.visitor.QueryPlanVisitor;
import org.opendatadiscovery.oddrn.model.OddrnPath;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils.fromSeq;

@Slf4j
@RequiredArgsConstructor
public class LogicalRelationVisitor extends QueryPlanVisitor<LogicalRelation> {
    private final SparkContext sparkContext;

    @Override
    public boolean isDefinedAt(final LogicalPlan logicalPlan) {
        return logicalPlan instanceof LogicalRelation
            && (((LogicalRelation) logicalPlan).relation() instanceof HadoopFsRelation
            || ((LogicalRelation) logicalPlan).relation() instanceof JDBCRelation
            || ((LogicalRelation) logicalPlan).catalogTable().isDefined());
    }

    @Override
    public LogicalPlanDependencies apply(final LogicalPlan logicalPlan) {
        final LogicalRelation logicalRelation = findLogicalRelation(logicalPlan);
        if (logicalRelation.relation() instanceof JDBCRelation) {
            return handleJdbcRelation((JDBCRelation) logicalRelation.relation());
        } else if (logicalRelation.relation() instanceof HadoopFsRelation) {
            final List<OddrnPath> inputs =
                fromSeq(((HadoopFsRelation) logicalRelation.relation()).location().rootPaths())
                    .stream()
                    .map(Path::toString)
                    .map(path -> OddrnUtils.resolveS3Oddrn(sparkContext.conf(), path))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());

            return LogicalPlanDependencies.inputs(inputs);
        }

        throw new IllegalArgumentException(String.format(
            "Expected logical plan to be JDBCRelation or HadoopFsRelation, but was %s", logicalPlan));
    }

    private LogicalRelation findLogicalRelation(final LogicalPlan logicalPlan) {
        if (logicalPlan instanceof LogicalRelation) {
            return (LogicalRelation) logicalPlan;
        }
        return findLogicalRelation(((UnaryNode) logicalPlan).child());
    }

    private LogicalPlanDependencies handleJdbcRelation(final JDBCRelation relation) {
        final String url = relation.jdbcOptions().url();
        final String tableOrQuery = relation.jdbcOptions().tableOrQuery();

        return LogicalPlanDependencies.inputs(
            extractTableNames(tableOrQuery)
                .stream()
                .map(tableName -> Utils.sqlOddrnPath(url, tableName))
                .collect(Collectors.toList())
        );
    }

    private List<String> extractTableNames(final String tableOrQuery) {
        try {
            final String sql = tableOrQuery.substring(tableOrQuery.indexOf("(") + 1, tableOrQuery.indexOf(")"));
            final Statement statement = CCJSqlParserUtil.parse(sql);

            return new TablesNamesFinder().getTableList(statement);
        } catch (final StringIndexOutOfBoundsException ignored) {
            log.info("JdbcRelation table found: {}", tableOrQuery);
        } catch (final JSQLParserException e) {
            log.warn("JDBCRelation ", e);
        }

        return Collections.emptyList();
    }
}
