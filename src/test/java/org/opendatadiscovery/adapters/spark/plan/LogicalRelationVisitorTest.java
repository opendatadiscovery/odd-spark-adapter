package org.opendatadiscovery.adapters.spark.plan;

import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.junit.jupiter.api.Test;
import org.opendatadiscovery.client.model.DataEntity;
import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogicalRelationVisitorTest {

    @Test
    public void testLogicalRelationDefined() {
        final LogicalRelation logicalRelation = mock(LogicalRelation.class);
        final SparkContext sparkContext = mock(SparkContext.class);
        final LogicalRelationVisitor visitor = new LogicalRelationVisitor(sparkContext);
        assertTrue(visitor.isDefinedAt(logicalRelation));
        final UnaryNode unaryNode = mock(UnaryNode.class);
        assertTrue(visitor.isDefinedAt(unaryNode));
    }

    @Test
    public void testLogicalRelationJdbcRelationOutput() {
        final JDBCRelation relation = mock(JDBCRelation.class);
        when(relation.jdbcOptions()).thenReturn(mock(JDBCOptions.class));
        when(relation.jdbcOptions().url()).thenReturn("jdbc:mysql://source-db:3306/mta_data");
        when(relation.jdbcOptions().tableOrQuery()).thenReturn("mta_reports");
        when(relation.jdbcOptions().parameters()).thenReturn(mock(CaseInsensitiveMap.class));
        when(relation.jdbcOptions().parameters().get(JDBCOptions.JDBC_TABLE_NAME()))
                .thenReturn(Option.apply("mta_reports"));
        when(relation.jdbcOptions().parameters().get(JDBCOptions.JDBC_QUERY_STRING()))
                .thenReturn(Option.apply("select * from foo"));
        assertEquals("jdbc:mysql://source-db:3306/mta_data", relation.jdbcOptions().url());
        assertEquals("mta_reports", relation.jdbcOptions().parameters().get(JDBCOptions.JDBC_TABLE_NAME()).get());
        final LogicalRelation logicalPlan = mock(LogicalRelation.class);
        when(logicalPlan.relation()).thenReturn(relation);
        final UnaryNode unaryNode = mock(UnaryNode.class);
        when(unaryNode.child()).thenReturn(logicalPlan);
        final SparkContext sparkContext = mock(SparkContext.class);
        final LogicalRelationVisitor visitor = new LogicalRelationVisitor(sparkContext);
        final List<DataEntity> data = visitor.apply(unaryNode);
        assertTrue(data.stream().anyMatch(d -> d.getOddrn()
                .equals("//mysql/host/source-db/databases/mta_data/tables/mta_reports")));
    }
}
