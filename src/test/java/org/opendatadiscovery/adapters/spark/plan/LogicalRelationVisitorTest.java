package org.opendatadiscovery.adapters.spark.plan;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.junit.jupiter.api.Test;
import scala.Option;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogicalRelationVisitorTest {

    @Test
    public void testLogicalRelationDefined() {
        var logicalRelation = mock(LogicalRelation.class);
        var sparkContext = mock(SparkContext.class);
        var visitor = new LogicalRelationVisitor(sparkContext);
        assertTrue(visitor.isDefinedAt(logicalRelation));
        var unaryNode = mock(UnaryNode.class);
        assertTrue(visitor.isDefinedAt(unaryNode));
    }

    @Test
    public void testLogicalRelationJdbcRelationOutput() {
        var sparkContext = mock(SparkContext.class);
        var visitor = new LogicalRelationVisitor(sparkContext);
        var unaryNode = mock(UnaryNode.class);
        var relation = mock(JDBCRelation.class);
        when(relation.jdbcOptions()).thenReturn(mock(JDBCOptions.class));
        when(relation.jdbcOptions().url()).thenReturn("jdbc:mysql://source-db:3306/mta_data");
        when(relation.jdbcOptions().parameters()).thenReturn(mock(CaseInsensitiveMap.class));
        when(relation.jdbcOptions().parameters().get(JDBCOptions.JDBC_TABLE_NAME()))
                .thenReturn(Option.apply("mta_reports"));
        when(relation.jdbcOptions().parameters().get(JDBCOptions.JDBC_QUERY_STRING()))
                .thenReturn(Option.apply("select * from foo"));
        assertEquals("jdbc:mysql://source-db:3306/mta_data", relation.jdbcOptions().url());
        assertEquals("mta_reports", relation.jdbcOptions().parameters().get(JDBCOptions.JDBC_TABLE_NAME()).get());
        var logicalPlan = mock(LogicalRelation.class);
        when(logicalPlan.relation()).thenReturn(relation);
        when(unaryNode.child()).thenReturn(logicalPlan);
        var data = visitor.apply(unaryNode);
        assertTrue(data.stream().anyMatch(d -> d.getOddrn()
                .equals("//mysql/host/source-db/databases/mta_data/tables/mta_reports")));
    }
}
