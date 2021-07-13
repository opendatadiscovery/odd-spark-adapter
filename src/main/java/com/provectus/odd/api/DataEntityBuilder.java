package com.provectus.odd.api;

import java.util.List;

import static com.provectus.odd.api.DataEntity.JOB;
import static com.provectus.odd.api.DataEntity.JOB_RUN;

public class DataEntityBuilder {
    DataEntity underConstruction = new DataEntity();

    public DataEntityBuilder type(String aType) {
        underConstruction.type = aType;
        if (aType.equals(JOB)) {
            if (underConstruction.data_transformer_run != null) {
                throw new IllegalArgumentException("Already set it to DataTranformerRun");
            }
            if (underConstruction.data_transformer == null) {
                underConstruction.data_transformer = new DataTransformer();
            }
        } else if (aType.equals(JOB_RUN)) {
            if (underConstruction.data_transformer != null) {
                throw new IllegalArgumentException("Already set it to DataTranformer");
            }
            if (underConstruction.data_transformer_run == null) {
                underConstruction.data_transformer_run = new DataTransformerRun();
            }
        } else {
            throw new IllegalArgumentException("Unknown type: " + aType);
        }
        return this;
    }

    public DataEntity build() {
        return underConstruction;
    }

    public DataEntityBuilder source_code_url(String sourceCodeUrl) {
        ensureCorrectType(JOB);
        underConstruction.data_transformer.source_code_url = sourceCodeUrl;
        return this;
    }

    public DataEntityBuilder name(String aName) {
        underConstruction.name = aName;
        return this;
    }

    public DataEntityBuilder sql(String aSql) {
        ensureCorrectType(JOB);
        underConstruction.data_transformer.sql = aSql;
        return this;
    }

    public DataEntityBuilder oddrn(String aOddrn) {
        underConstruction.oddrn = aOddrn;
        return this;
    }

    public DataEntityBuilder description(String aDescription) {
        underConstruction.description = aDescription;
        return this;
    }

    public DataEntityBuilder owner(String aOwner) {
        underConstruction.owner = aOwner;
        return this;
    }

    public DataEntityBuilder inputs(List<String> aInputs) {
        ensureCorrectType(JOB);
        underConstruction.data_transformer.inputs = aInputs;
        return this;
    }

    public DataEntityBuilder outputs(List<String> aOutputs) {
        ensureCorrectType(JOB);
        underConstruction.data_transformer.outputs = aOutputs;
        return this;
    }

    private void ensureCorrectType(String expectedType) {
        if (underConstruction.type == null || !underConstruction.type.equals(expectedType)) {
            throw new IllegalArgumentException("Should be DataTransformer to do this");
        }
    }

    public DataEntityBuilder status(String aStatus) {
        ensureCorrectType(JOB_RUN);
        underConstruction.data_transformer_run.status = aStatus;
        return this;
    }

    public DataEntityBuilder transformer_oddrn(String transformerOddrn) {
        ensureCorrectType(JOB_RUN);
        underConstruction.data_transformer_run.transformer_oddrn = transformerOddrn;
        return this;
    }

    public DataEntityBuilder start_time(String aStartTime) {
        ensureCorrectType(JOB_RUN);
        underConstruction.data_transformer_run.start_time = aStartTime;
        return this;
    }

    public DataEntityBuilder end_time(String aEndTime) {
        ensureCorrectType(JOB_RUN);
        underConstruction.data_transformer_run.end_time = aEndTime;
        return this;
    }

    public DataEntityBuilder status_reason(String aStatusReason) {
        ensureCorrectType(JOB_RUN);
        underConstruction.data_transformer_run.status_reason = aStatusReason;
        return this;
    }
}
