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

//    @Test
    public void testToString() {
        DataEntity dt = DataEntity.builder().name("zzz").type(JOB).sql(null).build();
        String actual = dt.toString();
        assertEquals(
                "DataEntity(super=BaseObject(oddrn=null, name=zzz, description=null, owner=null, type=JOB, " +
                        "meta_data=Metadata()), dataTransformer=DataTransformer(source_code_url=null, sql=null, " +
                        "inputs=null, outputs=null), dataTransformerRun=null)",
                actual);
    }

    @Test
    public void testStartTime() {
        DataEntityBuilder builder = DataEntity.builder();
        DataEntity actual = builder.type(JOB_RUN).start_time("123456").type(JOB_RUN).build();
        assertEquals("123456", actual.data_transformer_run.start_time);
    }
}
