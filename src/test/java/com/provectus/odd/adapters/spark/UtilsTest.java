package com.provectus.odd.adapters.spark;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.provectus.odd.api.DataEntity;
import org.junit.Test;

import java.util.HashMap;

import static com.provectus.odd.api.DataEntity.JOB_RUN;
import static org.junit.Assert.assertEquals;

public class UtilsTest {

    @Test
    public void testToJsonString() {
        HashMap<String, String> metadata = new HashMap<String, String>() {{
            put("a", "b");
            put("c", "d");
        }};
        DataEntity entity = DataEntity.builder().type(JOB_RUN).start_time("1234").metadata(metadata).build();
        String actual = Utils.toJsonString(entity);
        assertEquals("{\n" +
                "  \"data_transformer\": null,\n" +
                "  \"data_transformer_run\": {\n" +
                "    \"transformer_oddrn\": null,\n" +
                "    \"start_time\": \"1234\",\n" +
                "    \"end_time\": null,\n" +
                "    \"status_reason\": null,\n" +
                "    \"status\": null\n" +
                "  },\n" +
                "  \"oddrn\": null,\n" +
                "  \"name\": null,\n" +
                "  \"description\": null,\n" +
                "  \"owner\": null,\n" +
                "  \"type\": \"JOB_RUN\",\n" +
                "  \"metadata\": {\n" +
                "    \"a\": \"b\",\n" +
                "    \"c\": \"d\"\n" +
                "  }\n" +
                "}", actual);
     }

     @Test
     public void testTimestampToString() {
         String actual = Utils.timestampToString(1000000);
         assertEquals("1970-01-01T00:16:40+0000", actual);
     }

}
