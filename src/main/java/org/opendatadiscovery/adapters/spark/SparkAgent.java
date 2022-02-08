package org.opendatadiscovery.adapters.spark;

import java.lang.instrument.Instrumentation;
import org.opendatadiscovery.adapters.spark.transformers.PairRDDFunctionsTransformer;
import org.opendatadiscovery.adapters.spark.transformers.SparkContextTransformer;

public class SparkAgent {
    @SuppressWarnings("unused")
    public static void premain(final String agentArgs, final Instrumentation inst) {
        System.out.println("SparkAgent.premain: " + agentArgs + ", " + inst);
        OddAdapterSparkListener.setProperties(agentArgs);
        instrument(inst);
    }

    /** Entry point when attaching after application start. */
    @SuppressWarnings("unused")
    public static void agentmain(final String agentArgs, final Instrumentation inst) {
        premain(agentArgs, inst);
    }

    public static void instrument(final Instrumentation inst) {
        inst.addTransformer(new SparkContextTransformer());
        inst.addTransformer(new PairRDDFunctionsTransformer());
    }
}
