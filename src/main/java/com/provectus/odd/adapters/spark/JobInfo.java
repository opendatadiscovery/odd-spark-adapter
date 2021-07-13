package com.provectus.odd.adapters.spark;

import com.provectus.odd.api.DataEntityBuilder;
import org.apache.hadoop.conf.Configuration;

public class JobInfo {
    Configuration conf;
    private DataEntityBuilder dataTransformerBuilder = null;
    private DataEntityBuilder dataTransformerRunBuilder = null;

    public JobInfo() {
    }

    public JobInfo(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public void setDataTransformerBuilder(DataEntityBuilder aDataTransformerBuilder) {
        dataTransformerBuilder = aDataTransformerBuilder;
    }

    public void setDataTransformerRunBuilder(DataEntityBuilder aDataTransformerRunBuilder) {
        dataTransformerRunBuilder = aDataTransformerRunBuilder;
    }

    public void endedAt(long time) {
        String offsetTime = Utils.timestampToString(time);
        dataTransformerRunBuilder.end_time(offsetTime);
    }

    public DataEntityBuilder getDataTransformerBuilder() {
        return dataTransformerRunBuilder;
    }
}
