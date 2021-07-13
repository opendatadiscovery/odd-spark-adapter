package com.provectus.odd.api;

import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Getter
@ToString
public class DataTransformerRun {
    String transformer_oddrn;
    String start_time; // TODO use specific type and make sure it is serialized properly
    String end_time; // TODO
    String status_reason;
    String status; // TODO enum

    public DataTransformerRun() {
    }
}
