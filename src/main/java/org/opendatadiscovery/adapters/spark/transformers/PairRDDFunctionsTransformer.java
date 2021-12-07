package org.opendatadiscovery.adapters.spark.transformers;

import org.opendatadiscovery.adapters.spark.OddAdapterSparkListener;
import javassist.ClassPool;
import javassist.CtClass;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

@Slf4j
public class PairRDDFunctionsTransformer implements ClassFileTransformer {
    private static final String className = "org.apache.spark.rdd.PairRDDFunctions";
    private final String internalForm = className.replaceAll("\\.", "/");

    public static final String CODE =
            String.format("{ %s.registerOutput(this, conf); }", OddAdapterSparkListener.class.getName());

    @Override
    public byte[] transform(
            ClassLoader loader,
            String className,
            Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain,
            byte[] classfileBuffer)
            throws IllegalClassFormatException {
        if (!className.equals(this.internalForm)) {
            return classfileBuffer;
        }
        log.info("PairRDDFunctionsTransformer.transform({})", className);
        try {
            CtClass ctClass = ClassPool.getDefault().makeClass(new ByteArrayInputStream(classfileBuffer));
            ctClass.getDeclaredMethod("saveAsNewAPIHadoopDataset").insertBefore(CODE);
            ctClass.getDeclaredMethod("saveAsHadoopDataset").insertBefore(CODE);
            //ctClass.getDeclaredMethod("saveAsNewAPIHadoopFile").insertBefore(CODE);
            //ctClass.getDeclaredMethod("saveAsHadoopFile").insertBefore(CODE);
            return ctClass.toBytecode();
        } catch (Throwable throwable) {
            log.error("Failed to instrument " + className + ". Not doing anything", throwable);
            return classfileBuffer;
        }
    }
}
