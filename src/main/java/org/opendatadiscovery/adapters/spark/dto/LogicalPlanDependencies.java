package org.opendatadiscovery.adapters.spark.dto;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opendatadiscovery.oddrn.model.OddrnPath;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Data
@RequiredArgsConstructor
public class LogicalPlanDependencies {
    private final List<OddrnPath> inputs;
    private final List<OddrnPath> outputs;

    public static LogicalPlanDependencies merge(final Collection<LogicalPlanDependencies> deps) {
        final List<OddrnPath> inputs = new ArrayList<>();
        final List<OddrnPath> outputs = new ArrayList<>();

        for (final LogicalPlanDependencies dep : deps) {
            inputs.addAll(dep.inputs);
            outputs.addAll(dep.outputs);
        }

        return new LogicalPlanDependencies(inputs, outputs);
    }

    public static LogicalPlanDependencies inputs(final List<OddrnPath> inputs) {
        return new LogicalPlanDependencies(inputs, Collections.emptyList());
    }

    public static LogicalPlanDependencies outputs(final List<OddrnPath> outputs) {
        return new LogicalPlanDependencies(Collections.emptyList(), outputs);
    }

    public static LogicalPlanDependencies output(final OddrnPath output) {
        return new LogicalPlanDependencies(Collections.emptyList(), Collections.singletonList(output));
    }

    public static LogicalPlanDependencies empty() {
        return new LogicalPlanDependencies(Collections.emptyList(), Collections.emptyList());
    }
}