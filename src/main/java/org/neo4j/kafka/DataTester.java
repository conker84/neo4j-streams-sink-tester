package org.neo4j.kafka;

enum SinkType { AVRO, JSON }

public interface DataTester {

    void close();

    void send(String topic);

}
