package org.opendatadiscovery.adapters.spark.dto;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Data
@RequiredArgsConstructor
public class LogicalPlanDependencies {
    private final List<String> inputs;
    private final List<String> outputs;

    public static LogicalPlanDependencies merge(final Collection<LogicalPlanDependencies> deps) {
        final List<String> inputs = new ArrayList<>();
        final List<String> outputs = new ArrayList<>();

        for (final LogicalPlanDependencies dep : deps) {
            inputs.addAll(dep.inputs);
            outputs.addAll(dep.outputs);
        }

        return new LogicalPlanDependencies(inputs, outputs);
    }

    public static LogicalPlanDependencies inputs(final List<String> inputs) {
        return new LogicalPlanDependencies(inputs, Collections.emptyList());
    }

    public static LogicalPlanDependencies outputs(final List<String> outputs) {
        return new LogicalPlanDependencies(Collections.emptyList(), outputs);
    }

    public static LogicalPlanDependencies empty() {
        return new LogicalPlanDependencies(Collections.emptyList(), Collections.emptyList());
    }
}