package com.provectus.odd.api;

import org.junit.Test;

import static com.provectus.odd.api.DataEntity.JOB;
import static com.provectus.odd.api.DataEntity.JOB_RUN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DataTransformerTest {
    @Test
    public void testBuilder() {
        DataEntity dt = DataEntity.builder().name("zzz").type(JOB).sql(null).build();
        assertEquals("zzz", dt.name);
        assertNull(dt.data_transformer.getSql());
    }

    @Test
    public void testStartTime() {
        DataEntityBuilder builder = DataEntity.builder();
        DataEntity actual = builder.type(JOB_RUN).start_time("123456").type(JOB_RUN).build();
        assertEquals("123456", actual.data_transformer_run.start_time);
    }
}
