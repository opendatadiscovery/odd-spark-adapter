package org.opendatadiscovery.adapters.spark.transformers;

import java.io.ByteArrayInputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.adapters.spark.OddAdapterSparkListener;

@Slf4j
public class SparkContextTransformer implements ClassFileTransformer {
    private final String className = "org.apache.spark.SparkContext";
    private final String internalForm = className.replaceAll("\\.", "/");

    public static final String CODE =
            String.format("{ %s.instrument(this); }", OddAdapterSparkListener.class.getName());

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
        log.info("SparkContextTransformer.transform({})", className);
        try {
            final CtClass ctClass =
                    ClassPool.getDefault().makeClass(new ByteArrayInputStream(classfileBuffer), true);

            final CtConstructor[] constructors = ctClass.getConstructors();
            for (final CtConstructor constructor : constructors) {
                if (constructor.callsSuper()) {
                    constructor.insertAfter(CODE);
                }
            }
            return ctClass.toBytecode();
        } catch (Throwable throwable) {
            log.error("Failed to instrument " + className + ". Not doing anything", throwable);
            return classfileBuffer;
        }
    }
}

