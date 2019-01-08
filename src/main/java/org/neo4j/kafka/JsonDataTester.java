package org.neo4j.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class JsonDataTester implements DataTester {
    private final Properties props;
    private final KafkaProducer producer;
    private final Random rnd;

    private static final List<String> SURNAMES = Arrays.asList("Surname A", "Surname B", "Surname C", "Surname D", "Surname E");

    private static final String TEMPLATE_DATA = "{\"name\": \"Name %d\", \"surname\": \"%s\"}";

    public JsonDataTester(String kafkaBootstrapServer, String lingerMs, String batchSize) {
        this.props = new Properties();
        props.put("group.id", "neo4j");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        this.producer = new KafkaProducer(props);
        this.rnd = new Random();
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    @Override
    public void send(String topic) {
        int id = rnd.nextInt();
        try {
            String data = String.format(TEMPLATE_DATA, id, SURNAMES.get(rnd.nextInt(5)));
            String key = UUID.randomUUID().toString();
            ProducerRecord<String, byte[]> record = new ProducerRecord(topic, key,
                    data.getBytes());
            producer.send(record).get();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
