package com.provectus.odd.adapters.spark;

import java.io.ByteArrayInputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.function.Function;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PairRDDFunctionsTransformer implements ClassFileTransformer {
    private static final String className = "org.apache.spark.rdd.PairRDDFunctions";
    private final String internalForm = className.replaceAll("\\.", "/");

    public static final String CODE =
            String.format("{ %s.registerOutput(this, conf); }", SparkListener.class.getName());

    public static final String MYCODE = "{System.out.println(\"Instrumented!\"); }";

    void tryTransform(CtMethod method, TransformFunc func) {
        try {
            func.doTransform(method);
            log.error("Instrumented " + method + ".");
        } catch (Throwable throwable) {
            log.error("Failed to instrument " + method + ". Skipping.");
        }
    }

    void tryTransform(CtMethod[] methods, TransformFunc func) {
        for (CtMethod method : methods) {
            tryTransform(method, func);
        }
    }


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
        log.info("PairRDDFunctionsTransformer.transform(" + className + ")");
        try {
            CtClass ctClass = ClassPool.getDefault().makeClass(new ByteArrayInputStream(classfileBuffer));
            tryTransform(ctClass.getDeclaredMethods("saveAsNewAPIHadoopDataset"), (m)->m.insertBefore(CODE));
            tryTransform(ctClass.getDeclaredMethods("saveAsNewAPIHadoopFile"), (m)->m.insertBefore(CODE));
            tryTransform(ctClass.getDeclaredMethods("saveAsHadoopDataset"), (m)->m.insertBefore(CODE));
            tryTransform(ctClass.getDeclaredMethods("saveAsHadoopFile"), (m)->m.insertBefore(CODE));
            return ctClass.toBytecode();
        } catch (Throwable throwable) {
            log.error("Failed to instrument " + className + ". Not doing anything", throwable);
            return classfileBuffer;
        }
    }
}
