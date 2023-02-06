package org.opendatadiscovery.adapters.spark.dto;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.List;

@Data
@RequiredArgsConstructor
public class LogicalPlanDependencies {
    private final List<String> inputs;
    private final List<String> outputs;

    public static LogicalPlanDependencies inputs(final List<String> inputs) {
        return new LogicalPlanDependencies(inputs, Collections.emptyList());
    }

    public static LogicalPlanDependencies outputs(final List<String> outputs) {
        return new LogicalPlanDependencies(Collections.emptyList(), outputs);
    }
}