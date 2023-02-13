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
    private final List<? extends OddrnPath> inputs;
    private final List<? extends OddrnPath> outputs;

    public static LogicalPlanDependencies merge(final Collection<LogicalPlanDependencies> deps) {
        final List<OddrnPath> inputs = new ArrayList<>();
        final List<OddrnPath> outputs = new ArrayList<>();

        for (final LogicalPlanDependencies dep : deps) {
            inputs.addAll(dep.inputs);
            outputs.addAll(dep.outputs);
        }

        return new LogicalPlanDependencies(inputs, outputs);
    }

    public static <T extends OddrnPath> LogicalPlanDependencies inputs(final List<T> inputs) {
        return new LogicalPlanDependencies(inputs, Collections.emptyList());
    }

    public static <T extends OddrnPath> LogicalPlanDependencies outputs(final List<T> outputs) {
        return new LogicalPlanDependencies(Collections.emptyList(), outputs);
    }

    public static <T extends OddrnPath> LogicalPlanDependencies output(final T output) {
        return new LogicalPlanDependencies(Collections.emptyList(), Collections.singletonList(output));
    }

    public static LogicalPlanDependencies empty() {
        return new LogicalPlanDependencies(Collections.emptyList(), Collections.emptyList());
    }
}