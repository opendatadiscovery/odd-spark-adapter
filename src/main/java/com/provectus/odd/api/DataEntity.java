package com.provectus.odd.api;

import lombok.ToString;

@ToString(callSuper = true)
public class DataEntity extends BaseObject {

    public static final String JOB = "JOB";
    public static final String JOB_RUN = "JOB_RUN";

    DataTransformer data_transformer = null;
    DataTransformerRun data_transformer_run = null;

    public static DataEntityBuilder builder() {
        return new DataEntityBuilder();
    }


    public DataTransformer getData_transformer() {
        return data_transformer;
    }

    public DataTransformerRun getData_transformer_run() {
        return data_transformer_run;
    }

    public DataEntity() {
    }
}
