package com.provectus.odd.adapters.spark;

import com.provectus.odd.api.DataEntity;
import org.junit.Test;

import static com.provectus.odd.api.DataEntity.JOB_RUN;
import static org.junit.Assert.assertEquals;

public class UtilsTest {
//    @Test
    public void testJson() {
        String actual = Utils.toJsonString(new TestEntity());
        assertEquals("{\n  \"thing\": \"zzz\"\n}", actual);
    }

    @Test
    public void testToJsonString() {
        DataEntity entity = DataEntity.builder().type(JOB_RUN).start_time("1234").build();
        String actual = Utils.toJsonString(entity);
        assertEquals("{\n" +
                "  \"oddrn\" : null,\n" +
                "  \"name\" : null,\n" +
                "  \"description\" : null,\n" +
                "  \"owner\" : null,\n" +
                "  \"type\" : \"JOB_RUN\",\n" +
                "  \"metadata\" : { },\n" +
                "  \"data_transformer\" : null,\n" +
                "  \"data_transformer_run\" : {\n" +
                "    \"transformer_oddrn\" : null,\n" +
                "    \"start_time\" : \"1234\",\n" +
                "    \"end_time\" : null,\n" +
                "    \"status_reason\" : null,\n" +
                "    \"status\" : null\n" +
                "  }\n" +
                "}", actual);
    }
}
