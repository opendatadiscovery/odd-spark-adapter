package com.provectus.odd.api;

import lombok.ToString;

@ToString(callSuper = true)
public class DataEntity extends BaseObject {

    public static final String JOB = "JOB";
    public static final String JOB_RUN = "JOB_RUN";

    DataTransformer dataTransformer = null;
    DataTransformerRun dataTransformerRun = null;

    public static DataEntityBuilder builder() {
        return new DataEntityBuilder();
    }


    public DataTransformer getDataTransformer() {
        return dataTransformer;
    }

    public DataTransformerRun getDataTransformerRun() {
        return dataTransformerRun;
    }

    public DataEntity() {
    }
}
