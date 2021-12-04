package com.provectus.odd.adapters.spark.mapper;

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.immutable.Map;

import static com.provectus.odd.adapters.spark.mapper.DataTransformerMapper.DBTABLE;
import static com.provectus.odd.adapters.spark.mapper.DataTransformerMapper.URL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class DataTransformerMapperTest {

    @Test
    public void dataTransformerSqlInputsTest() {
        var relation = mock(JDBCRelation.class);
        when(relation.jdbcOptions()).thenReturn(mock(JDBCOptions.class));
        when(relation.jdbcOptions().url()).thenReturn("jdbc:mysql://source-db:3306/mta_data");
        when(relation.jdbcOptions().parameters()).thenReturn(mock(CaseInsensitiveMap.class));
        when(relation.jdbcOptions().parameters().get(JDBCOptions.JDBC_TABLE_NAME()))
                .thenReturn(Option.apply("mta_reports"));
        assertEquals("jdbc:mysql://source-db:3306/mta_data", relation.jdbcOptions().url());
        assertEquals("mta_reports", relation.jdbcOptions().parameters().get(JDBCOptions.JDBC_TABLE_NAME()).get());
        var inputs = new DataTransformerMapper().map(relation);
        assertTrue(inputs.getInputs().contains("//mysql/host/source-db:3306/databases/mta_data/tables/mta_reports"));
    }

    @Test
    public void dataTransformerSqlOutputsTest() {
        var command = mock(SaveIntoDataSourceCommand.class);
        assertNotNull(command);
        when(command.options()).thenReturn(mock(Map.class));
        assertNotNull(command.options());
        when(command.options().get(URL)).thenReturn(Option.apply("jdbc:postgresql://target-db:5432/mta_data"));
        when(command.options().get(DBTABLE)).thenReturn(Option.apply("mta_transform"));
        assertEquals("jdbc:postgresql://target-db:5432/mta_data", command.options().get(URL).get());
        assertEquals("mta_transform", command.options().get(DBTABLE).get());
        var outputs = new DataTransformerMapper().map(command);
        assertTrue(outputs.getOutputs()
                .contains("//postgresql/host/target-db:5432/databases/mta_data/schemas/public/tables/mta_transform"));
    }
}
