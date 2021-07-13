package com.provectus.odd.api;

import lombok.Getter;
import lombok.ToString;

import java.util.List;

@Getter
@ToString
public class DataTransformer {
    String source_code_url;
    String sql;
    List<String> inputs;
    List<String> outputs;
}
