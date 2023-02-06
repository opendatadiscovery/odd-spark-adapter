package org.opendatadiscovery.adapters.spark.transformers;

import java.io.ByteArrayInputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import javassist.ClassPool;
import javassist.CtClass;
import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.adapters.spark.ODDSparkListener;

@Slf4j
public class PairRDDFunctionsTransformer implements ClassFileTransformer {
    private static final String CLASS_NAME = "org.apache.spark.rdd.PairRDDFunctions";
    private final String internalForm = CLASS_NAME.replaceAll("\\.", "/");

    public static final String CODE =
            String.format("{ %s.registerOutput(this, conf); }", ODDSparkListener.class.getName());

    @Override
    public byte[] transform(
            final ClassLoader loader,
            final String className,
            final Class<?> classBeingRedefined,
            final ProtectionDomain protectionDomain,
            final byte[] classfileBuffer)
            throws IllegalClassFormatException {
        if (!className.equals(this.internalForm)) {
            return classfileBuffer;
        }
        log.info("PairRDDFunctionsTransformer.transform({})", className);
        try {
            final CtClass ctClass = ClassPool.getDefault().makeClass(new ByteArrayInputStream(classfileBuffer));
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
