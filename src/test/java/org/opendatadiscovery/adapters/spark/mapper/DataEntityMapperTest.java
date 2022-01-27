package org.opendatadiscovery.adapters.spark.mapper;

import org.apache.spark.scheduler.SparkListenerJobStart;
import org.junit.jupiter.api.Test;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityType;

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataEntityMapperTest {

    @Test
    public void jobDateEntityMapperTest() {
        DataEntity jobRunDataEntity = mockSparkListenerJobStart();
        DataEntity jobDataEntity = DataEntityMapper.map(jobRunDataEntity);
        assertEquals(DataEntityType.JOB, jobDataEntity.getType());
        assertEquals("//spark/host/spark-master/jobs/etl-app",
                jobDataEntity.getOddrn());
    }

    @Test
    public void jobRunDateEntityMapperTest() {
        DataEntity dataEntity = mockSparkListenerJobStart();
        assertEquals("//spark/host/spark-master/jobs/etl-app/runs/app-20211204075250-0013",
                dataEntity.getOddrn());
        assertEquals(DataEntityType.JOB_RUN, dataEntity.getType());
        assertNotNull(dataEntity.getDataTransformerRun().getStartTime());
        assertEquals("//spark/host/spark-master/jobs/etl-app",
                dataEntity.getDataTransformerRun().getTransformerOddrn());
    }

    private DataEntity mockSparkListenerJobStart() {
        SparkListenerJobStart jobStart = mock(SparkListenerJobStart.class);
        when(jobStart.time()).thenReturn(Instant.now().toEpochMilli());
        when(jobStart.properties()).thenReturn(mock(Properties.class));
        when(jobStart.properties().getProperty(DataEntityMapper.SPARK_MASTER)).thenReturn("spark://spark-master:7077");
        when(jobStart.properties().getProperty(DataEntityMapper.SPARK_APP_NAME)).thenReturn("etl-app");
        when(jobStart.properties().getProperty(DataEntityMapper.SPARK_APP_ID)).thenReturn("app-20211204075250-0013");
        return DataEntityMapper.map(jobStart.properties());
    }
}
