package org.opendatadiscovery.adapters.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OddSparkAdapterIT {

    @SneakyThrows
    @Test
    public void sqlJobTest() {
        final String outputFile = "sqlJobTest-" + UUID.randomUUID();
        final Process proc = sparkSubmit("apps/mysql_pg_job.py",
                outputFile,
                "apps/postgresql-42.2.22.jar,apps/mysql-connector-java-8.0.26.jar");
        logging(proc, "sqlJobTest");
        final int exitCode = proc.waitFor();
        assertEquals(0, exitCode, "sqlJobTest finished with exit code: " + exitCode);
        assertLinesMatch(
                Files.lines(Paths.get("data/sqlJobTest.output")),
                Files.lines(Paths.get(outputFile)));
        assertTrue(new File(outputFile).delete());
    }

    @SneakyThrows
    @Test
    public void customS3JobTest() {
        final String outputFile = "customS3JobTest-" + UUID.randomUUID();
        final Process proc = sparkSubmit("apps/s3-custom-word-count.py",
                outputFile,
                "apps/aws-java-sdk-bundle-1.11.874.jar,apps/hadoop-aws-3.2.0.jar");
        logging(proc, "customS3JobTest");
        final int exitCode = proc.waitFor();
        assertEquals(0, exitCode, "customS3JobTest finished with exit code: " + exitCode);
        assertLinesMatch(
                Files.lines(Paths.get("data/customS3JobTest.output")),
                Files.lines(Paths.get(outputFile)));
        assertTrue(new File(outputFile).delete());
    }

    @SneakyThrows
    @Test
    public void hdfsJobTest() {
        final String outputFile = "hdfsJobTest-" + UUID.randomUUID();
        final Process proc = sparkSubmit("apps/word-count.py",
                outputFile);
        logging(proc, "hdfsJobTest");
        final int exitCode = proc.waitFor();
        assertEquals(1, exitCode, "hdfsJobTest finished with exit code: " + exitCode);
        assertLinesMatch(
                Files.lines(Paths.get("data/hdfsJobTest.output")),
                Files.lines(Paths.get(outputFile)));
        assertTrue(new File(outputFile).delete());
    }

    @SneakyThrows
    private Process sparkSubmit(final String appResource, final String outputFile, final String... jars) {
        final SparkLauncher spark = new SparkLauncher()
                .setVerbose(true)
                .setMaster("local")
                .setSparkHome("spark")
                .setAppResource(appResource)
                .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
                .setConf(SparkLauncher.EXECUTOR_MEMORY, "1g")
                .setConf(SparkLauncher.DRIVER_DEFAULT_JAVA_OPTIONS,
                        "-javaagent:build/libs/odd-spark-adapter-0.0.1.jar=file://" + outputFile);
        if (jars.length > 0) {
            spark.setConf("spark.jars", jars[0]);
        }
        return spark.launch();
    }

    private void logging(final Process proc, final String name) {
        CompletableFuture.runAsync(new InputStreamReaderRunnable(proc.getInputStream(), name + " input"));
        CompletableFuture.runAsync(new InputStreamReaderRunnable(proc.getErrorStream(), name));
    }

    static class InputStreamReaderRunnable implements Runnable {
        private final String name;
        private final BufferedReader reader;

        public InputStreamReaderRunnable(final InputStream is, final String name) {
            this.name = name;
            this.reader = new BufferedReader(new InputStreamReader(is));
        }

        @SneakyThrows
        @Override
        public void run() {
            try {
                String line = reader.readLine();
                while (line != null) {
                    System.out.println(name + ":" + line);
                    line = reader.readLine();
                }
            } catch (Exception e) {
                System.out.println(name + ":run() failed with error message: " + e.getMessage());
            } finally {
                reader.close();
            }
        }
    }
}
