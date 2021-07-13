package com.provectus.odd.adapters.spark.examples;

import com.provectus.odd.adapters.spark.SparkListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Pi {
    private static final int DEFAULT_SAMPLES = 1000;

    public static void main(String[] args) {
        System.out.println("Starting job...");
        SparkSession spark = SparkSession.builder()
                .appName("Pi2")
                .config("spark.extraListeners", "com.provectus.odd.adapters.spark.SparkListener")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        int numSamples = DEFAULT_SAMPLES;
        if (args.length >= 1) {
            numSamples = Integer.parseInt(args[0]);
        }

        List<Integer> l = new ArrayList<>(numSamples);
        for (int i = 0; i < numSamples; i++) {
            l.add(i);
        }

        JavaRDD<Tuple2<Double, Double>> rdd = sc.parallelize(l).map(
                (i) -> new Tuple2<Double, Double>(Math.random(), Math.random()));
        JavaPairRDD<Boolean, Integer> rdd2 = rdd.mapToPair((t) -> new Tuple2<Boolean, Integer>(t._1 * t._1 + t._2 * t._2 < 1, 1));
//        Configuration conf = sc.hadoopConfiguration();
//        conf.set("mapreduce.outputformat.class", "org.apache.hadoop.mapred.FileOutputFormat");
//        conf.set("mapreduce.output.fileoutputformat.outputdir", "pi3");
//        rdd2.saveAsNewAPIHadoopDataset(conf);
        rdd2.saveAsTextFile(args[1]);

//        System.out.println("Pi is roughly " + 4.0 * count / numSamples + " (took " + numSamples + " samples)");
        spark.stop();
    }
}
