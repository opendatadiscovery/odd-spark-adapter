package org.opendatadiscovery.adapters.spark.plan;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.opendatadiscovery.adapters.spark.VisitorFactoryImpl;
import org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import scala.collection.Seq;
import scala.runtime.AbstractPartialFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
// TODO: InsertIntoDataSourceCommand
public class SaveIntoDataSourceCommandVisitor extends QueryPlanVisitor<SaveIntoDataSourceCommand, String> {
    public static final String URL = "url";
    public static final String DBTABLE = "dbtable";

    @Override
    public List<String> apply(final LogicalPlan logicalPlan) {
        final SparkContext sparkContext = SparkContext$.MODULE$.getActive().get();
        final List<QueryPlanVisitor<? extends LogicalPlan, String>> inputVisitors
            = new VisitorFactoryImpl().getVisitors(sparkContext);

        final List<String> result = new ArrayList<>();

        try {
            final SaveIntoDataSourceCommand command = (SaveIntoDataSourceCommand) logicalPlan;

            final Seq<List<String>> seq = command.query()
                .collect(new AbstractPartialFunction<LogicalPlan, List<String>>() {
                    @Override
                    public boolean isDefinedAt(final LogicalPlan x) {
                        return inputVisitors.stream().anyMatch(iv -> iv.isDefinedAt(x));
                    }

                    @Override
                    public List<String> apply(final LogicalPlan lp) {
                        return inputVisitors.stream().filter(iv -> iv.isDefinedAt(lp))
                            .map(iv -> iv.apply(lp))
                            .peek(list -> log.info("OPA: {}", list))
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
                    }
                });

            result.addAll(ScalaConversionUtils.fromSeq(seq)
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));

            final String url = command.options().get(URL).get();
            final String tableName = command.options().get(DBTABLE).get();
            log.info("OPA: {}", Utils.sqlGenerator(url, tableName));
            result.add(Utils.sqlGenerator(url, tableName));
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }
}
