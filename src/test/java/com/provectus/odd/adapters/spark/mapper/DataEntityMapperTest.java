package com.provectus.odd.adapters.spark.mapper;

import org.apache.spark.scheduler.SparkListenerJobStart;
import org.junit.jupiter.api.Test;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityType;

import java.time.Instant;
import java.util.Properties;

import static com.provectus.odd.adapters.spark.mapper.DataEntityMapper.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataEntityMapperTest {

    @Test
    public void jobDateEntityMapperTest() {
        var jobRunDataEntity = mockSparkListenerJobStart();
        var jobDataEntity = new DataEntityMapper().map(jobRunDataEntity);
        assertEquals(DataEntityType.JOB, jobDataEntity.getType());
        assertEquals("//spark/host/spark-master:7077/jobs/etl-app",
                jobDataEntity.getOddrn());
    }

    @Test
    public void jobRunDateEntityMapperTest() {
        var dataEntity = mockSparkListenerJobStart();
        assertEquals("//spark/host/spark-master:7077/jobs/etl-app/runs/app-20211204075250-0013",
                dataEntity.getOddrn());
        assertEquals(DataEntityType.JOB_RUN, dataEntity.getType());
        assertNotNull(dataEntity.getDataTransformerRun().getStartTime());
        assertEquals("//spark/host/spark-master:7077/jobs/etl-app",
                dataEntity.getDataTransformerRun().getTransformerOddrn());
    }

    private DataEntity mockSparkListenerJobStart() {
        var jobStart = mock(SparkListenerJobStart.class);
        when(jobStart.time()).thenReturn(Instant.now().toEpochMilli());
        when(jobStart.properties()).thenReturn(mock(Properties.class));
        when(jobStart.properties().getProperty(SPARK_MASTER)).thenReturn("spark://spark-master:7077");
        when(jobStart.properties().getProperty(SPARK_APP_NAME)).thenReturn("etl-app");
        when(jobStart.properties().getProperty(SPARK_APP_ID)).thenReturn("app-20211204075250-0013");
        return new DataEntityMapper().map(jobStart);
    }
}
