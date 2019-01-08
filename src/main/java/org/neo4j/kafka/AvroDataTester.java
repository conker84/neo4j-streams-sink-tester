package org.neo4j.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class AvroDataTester implements DataTester {

    private final Properties props;
    private final KafkaProducer producer;


    private static final Schema schemaKey = new Schema.Parser().parse("{\"type\":\"record\"," +
            "\"name\":\"User\"," +
            "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}, {\"name\":\"surname\",\"type\":\"string\"}]}");

    private final Random rnd;

    private static final List<String> SURNAMES = Arrays.asList("Surname A", "Surname B", "Surname C", "Surname D", "Surname E");

    public AvroDataTester(String kafkaBootstrapServer, String kafkaSchemaRegistryUrl, String lingerMs,
                          String batchSize) {
        this.props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", kafkaSchemaRegistryUrl);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        this.producer = new KafkaProducer(props);
        this.rnd = new Random();
    }


    public void send(String topic) {
        int id = rnd.nextInt();
        GenericRecord avroRecordKey = new GenericData.Record(schemaKey);
        avroRecordKey.put("name", "Name " + id);
        avroRecordKey.put("surname", SURNAMES.get(rnd.nextInt(5)));
        try {
            ProducerRecord<GenericRecord, GenericRecord> record = new ProducerRecord<>(topic, avroRecordKey, avroRecordKey);
            producer.send(record).get();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}
