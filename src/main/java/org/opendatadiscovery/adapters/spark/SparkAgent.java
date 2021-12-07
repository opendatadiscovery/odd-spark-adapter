package org.opendatadiscovery.adapters.spark;

import org.opendatadiscovery.adapters.spark.transformers.PairRDDFunctionsTransformer;
import org.opendatadiscovery.adapters.spark.transformers.SparkContextTransformer;

import java.lang.instrument.Instrumentation;

public class SparkAgent {
    @SuppressWarnings("unused")
    public static void premain(String agentArgs, Instrumentation inst) {
        System.out.println("SparkAgent.premain: " + agentArgs + ", " + inst);
        instrument(inst);
    }

    /** Entry point when attaching after application start */
    @SuppressWarnings("unused")
    public static void agentmain(String agentArgs, Instrumentation inst) {
        premain(agentArgs, inst);
    }

    public static void instrument(Instrumentation inst) {
        inst.addTransformer(new SparkContextTransformer());
        inst.addTransformer(new PairRDDFunctionsTransformer());
    }
}
