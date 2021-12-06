package com.provectus.odd.adapters.spark.plan;

import com.provectus.odd.adapters.spark.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityType;
import org.opendatadiscovery.client.model.DataTransformer;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction0;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.provectus.odd.adapters.spark.utils.Utils.fileGenerator;
import static com.provectus.odd.adapters.spark.utils.Utils.namespaceUri;
import static com.provectus.odd.adapters.spark.utils.Utils.sqlGenerator;


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
                      // TODO- refactor this to return a single partitioned dataset based on static
                      // static partitions in the relation
                      var uri = path.toUri();
                      var namespace = namespaceUri(uri);
                      var patt = String.format("%s://%s", namespace, uri.getPath());
                      var fileName = Arrays.stream(relation.location().inputFiles())
                              .filter(f -> f.contains(patt))
                              .map(f -> f.replace(patt, "")).collect(Collectors.joining());
                      return new DataEntity()
                              .type(DataEntityType.FILE)
                              .oddrn(fileGenerator(namespace, uri.getPath(), fileName));
                    })
            .collect(Collectors.toList());
  }

  private List<DataEntity> handleJdbcRelation(JDBCRelation relation) {
    // TODO- if a relation is composed of a complex sql query, we should attempt to
    // extract the
    // table names so that we can construct a true lineage
    String tableName =
            relation
                    .jdbcOptions()
                    .parameters()
                    .get(JDBCOptions.JDBC_TABLE_NAME())
                    .getOrElse(
                            new AbstractFunction0<>() {
                              @Override
                              public String apply() {
                                return "COMPLEX";
                              }
                            });
    String sql =
            relation
                    .jdbcOptions()
                    .parameters()
                    .get(JDBCOptions.JDBC_QUERY_STRING())
                    .getOrElse(
                            new AbstractFunction0<>() {
                              @Override
                              public String apply() {
                                return "";
                              }
                            });
    return Collections.singletonList(new DataEntity()
            .type(DataEntityType.TABLE)
            .dataTransformer(new DataTransformer().sql(sql))
            .oddrn(sqlGenerator(relation.jdbcOptions().url(), tableName))
    );
  }
}
