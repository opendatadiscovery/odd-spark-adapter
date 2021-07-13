package com.provectus.odd.api;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class DataTransformerRun {
    String transformerOddrn;
    String startTime;
    String endTime;
    String statusReason;
    String status;

    public DataTransformerRun() {
    }
}
