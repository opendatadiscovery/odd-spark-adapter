package org.opendatadiscovery.adapters.spark.plan;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.client.model.DataEntity;
import scala.runtime.AbstractPartialFunction;

public abstract class QueryPlanVisitor<T extends LogicalPlan, D extends DataEntity>
        extends AbstractPartialFunction<LogicalPlan, List<D>> {

    @Override
    public boolean isDefinedAt(final LogicalPlan logicalPlan) {
        final Type genericSuperclass = getClass().getGenericSuperclass();
        if (!(genericSuperclass instanceof ParameterizedType)) {
            return false;
        }
        final Type[] typeArgs = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
        if (typeArgs != null && typeArgs.length > 0) {
            final Type arg = typeArgs[0];
            return ((Class) arg).isAssignableFrom(logicalPlan.getClass());
        }
        return false;
    }
}