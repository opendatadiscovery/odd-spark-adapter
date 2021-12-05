package com.provectus.odd.adapters.spark.plan;

import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.immutable.Map;

import static com.provectus.odd.adapters.spark.mapper.DataTransformerMapper.DBTABLE;
import static com.provectus.odd.adapters.spark.mapper.DataTransformerMapper.URL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SaveIntoDataSourceCommandVisitorTest {

    @Test
    public void testSaveIntoDataSourceCommandOutput() {
        var command = mock(SaveIntoDataSourceCommand.class);
        assertNotNull(command);
        when(command.options()).thenReturn(mock(Map.class));
        assertNotNull(command.options());
        when(command.options().get(URL)).thenReturn(Option.apply("jdbc:postgresql://target-db:5432/mta_data"));
        when(command.options().get(DBTABLE)).thenReturn(Option.apply("mta_transform"));
        assertEquals("jdbc:postgresql://target-db:5432/mta_data", command.options().get(URL).get());
        assertEquals("mta_transform", command.options().get(DBTABLE).get());
        var data = new SaveIntoDataSourceCommandVisitor().apply(command);
        assertTrue(data.stream().anyMatch(d -> d.getOddrn()
                .equals("//postgresql/host/target-db:5432/databases/mta_data/schemas/public/tables/mta_transform")));
    }

    @Test
    public void testSaveIntoDataSourceCommandDefined() {
        var command = mock(SaveIntoDataSourceCommand.class);
        assertNotNull(command);
        assertTrue(new SaveIntoDataSourceCommandVisitor().isDefinedAt(command));
    }
}
