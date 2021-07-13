package com.provectus.odd.adapters.spark;

import javassist.CannotCompileException;
import javassist.CtMethod;

public interface TransformFunc {
    void doTransform(CtMethod method) throws CannotCompileException;
}
