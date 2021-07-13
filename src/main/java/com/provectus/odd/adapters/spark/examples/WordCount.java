package com.provectus.odd.adapters.spark.examples;

import com.provectus.odd.adapters.spark.SparkListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import javax.naming.SizeLimitExceededException;
import java.util.Arrays;

@Slf4j
public class WordCount {
    public static void main(String[] args) {
        if (args.length < 2) {
            log.info("Usage: WordCount <source> <destination>");
        } else {
            String input = args[0];
            String output = args[1];
            log.info("Input: {}, output: {}", input, output);
            log.info("Starting job...");
            SparkSession spark = SparkSession.builder()
                    .appName("WordCount")
                    .config("spark.extraListeners", "com.provectus.odd.adapters.spark.SparkListener")
                    .config(SparkListener.ENDPOINT_CONFIG_KEY, "http://localhost:8080/ingestion/entities")
                    .getOrCreate();
            JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            JavaRDD<String> textFile = sc.textFile(args[0]);
            JavaPairRDD<String, Integer> counts = textFile
                    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey(Integer::sum);
            counts.saveAsTextFile(output);
        }
    }
}
