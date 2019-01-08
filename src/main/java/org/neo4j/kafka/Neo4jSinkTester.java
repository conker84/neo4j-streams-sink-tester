package org.neo4j.kafka;

import org.apache.commons.cli.*;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;


public class Neo4jSinkTester {

    public static void main(String[] args) throws Exception {
        String defaultEvents = "100";
        String defaultTopic = "my-topic";
        String defaultIterations = "3";
        String defaultSink = SinkType.JSON.toString();
        Options options = new Options();
        options.addOption("e", "events", true, String.format("Number of events to generate (default: %s)", defaultEvents))
            .addOption("t", "topic", true, String.format("Topic where publish (default: %s)", defaultTopic))
            .addOption("i","iterations", true, String.format("Number of iterations (only in report mode, default: %s)", defaultIterations))
            .addOption("h", "help", false, "Print this screen")
            .addOption("f", "format", true, String.format("The data format we want to test (AVRO, JSON). Default (%s)", defaultSink))
            .addOption("c","clearDB", false, "Clear the DB on each iteration (only in report mode)")
            .addOption("r","report", false, "Get the report of the inserts")
            .addOption(Option.builder("D")
                    .argName("property=value")
                    .numberOfArgs(2)
                    .hasArgs()
                    .valueSeparator()
                    .desc("use value for given property")
                    .build());
        CommandLineParser cliParser = new DefaultParser();
        CommandLine cmd = cliParser.parse(options, args);

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Neo4jSinkTester", options);
            return;
        }

        int events = Integer.parseInt(cmd.getOptionValue("e", defaultEvents));
        int iterations = Integer.parseInt(cmd.getOptionValue("i", defaultIterations));
        boolean isAppend = cmd.hasOption("a");
        boolean report = cmd.hasOption("r");

        String neo4jUri = cmd.getOptionProperties("D").getOrDefault("neo4j.uri", "bolt://localhost:7687").toString();
        String neo4jUser = cmd.getOptionProperties("D").getOrDefault("neo4j.user", "neo4j").toString();
        String neo4jPass = cmd.getOptionProperties("D").getOrDefault("neo4j.password", "connect").toString();

        String kafkaBootstrapServer = cmd.getOptionProperties("D").getOrDefault("kafka.bootstrap.server", "localhost:9092").toString();
        String kafkaSchemaRegistryUrl = cmd.getOptionProperties("D").getOrDefault("kafka.schema.registry.url", "http://localhost:8081").toString();
        String lingerMs = cmd.getOptionProperties("D").getOrDefault("kafka.linger.ms", "0").toString();
        String batchSize = cmd.getOptionProperties("D").getOrDefault("kafka.batch.size", "16384").toString();

        String topic = cmd.getOptionValue("t", defaultTopic);
        final DataTester loadTest;
        final String sinkType = cmd.getOptionValue("f", defaultSink);
        switch (SinkType.valueOf(sinkType)) {
            case AVRO:
                loadTest = new AvroDataTester(kafkaBootstrapServer, kafkaSchemaRegistryUrl, lingerMs, batchSize);
                break;
            case JSON:
                loadTest = new JsonDataTester(kafkaBootstrapServer, lingerMs, batchSize);
                break;
            default:
                throw new IllegalArgumentException("Check SinkType value");
        }
        System.out.println(String.format("You are sending data in %s format", sinkType));

        if (report) {
            try (Driver driver = GraphDatabase.driver(neo4jUri, AuthTokens.basic(neo4jUser, neo4jPass));
                 Session session = driver.session()) {
                final Map<String, Integer> times = new HashMap<String, Integer>(){{
                    put("maxTime", 0);
                    put("minTime", 0);
                    put("sum", 0);
                }};

                IntStream.range(0, iterations).forEach(it -> {
                    if (!isAppend) {
                        session.writeTransaction((tx) -> tx.run("MATCH (n) OPTIONAL match (n)-[r]-(m) DELETE n, r, m"));
                    }
                    int startCount = getCount(session);

                    long start = System.currentTimeMillis();
                    IntStream.range(0, events).forEach(i -> {
                        loadTest.send(topic);
                    });
                    int count = 0;
                    while (count - startCount < events) {
                        try {
                            Thread.sleep(1000);
                        } catch (Exception e) {}
                        count = getCount(session);
                    }
                    long end = System.currentTimeMillis();
                    int time = (int) ((end - start) / 1e3);
                    times.computeIfPresent("maxTime", (s, maxTime) -> time > maxTime ? time : maxTime);
                    times.computeIfPresent("minTime", (s, minTime) -> minTime == 0 || time < minTime ? time : minTime);
                    times.computeIfPresent("sum", (s, sum) -> sum + time);
                    System.out.println(String.format("Iteration %d, %d events loaded in %d seconds", (it + 1), events, time));
                });

                System.out.println(String.format("Max time %d", times.get("maxTime")));
                System.out.println(String.format("Min time %d", times.get("minTime")));
                System.out.println(String.format("Avg time %d", (int) (times.get("sum") / iterations)));
                loadTest.close();
            }
        } else {
            IntStream.range(0, events).forEach(i -> {
                loadTest.send(topic);
            });
            loadTest.close();
        }

        System.exit(0);
    }

    private static Integer getCount(Session session) {
        return session.readTransaction((tx) -> tx.run("MATCH (n) RETURN count(n) AS count").single().get("count").asInt());
    }
}

