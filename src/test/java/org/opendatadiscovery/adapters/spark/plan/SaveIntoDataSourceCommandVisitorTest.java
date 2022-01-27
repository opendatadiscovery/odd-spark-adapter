package org.opendatadiscovery.adapters.spark.plan;

import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.junit.jupiter.api.Test;
import org.opendatadiscovery.client.model.DataEntity;
import scala.Option;
import scala.collection.immutable.Map;

import java.util.List;

import static org.opendatadiscovery.adapters.spark.plan.SaveIntoDataSourceCommandVisitor.URL;
import static org.opendatadiscovery.adapters.spark.plan.SaveIntoDataSourceCommandVisitor.DBTABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SaveIntoDataSourceCommandVisitorTest {

    @Test
    public void testSaveIntoDataSourceCommandOutput() {
        SaveIntoDataSourceCommand command = mock(SaveIntoDataSourceCommand.class);
        assertNotNull(command);
        when(command.options()).thenReturn(mock(Map.class));
        assertNotNull(command.options());
        when(command.options().get(URL)).thenReturn(Option.apply("jdbc:postgresql://target-db:5432/mta_data"));
        when(command.options().get(DBTABLE)).thenReturn(Option.apply("mta_transform"));
        assertEquals("jdbc:postgresql://target-db:5432/mta_data", command.options().get(URL).get());
        assertEquals("mta_transform", command.options().get(DBTABLE).get());
        List<DataEntity> data = new SaveIntoDataSourceCommandVisitor().apply(command);
        assertTrue(data.stream().anyMatch(d -> d.getOddrn()
                .equals("//postgresql/host/target-db/databases/mta_data/schemas/public/tables/mta_transform")));
    }

    @Test
    public void testSaveIntoDataSourceCommandDefined() {
        SaveIntoDataSourceCommand command = mock(SaveIntoDataSourceCommand.class);
        assertNotNull(command);
        assertTrue(new SaveIntoDataSourceCommandVisitor().isDefinedAt(command));
    }
}
