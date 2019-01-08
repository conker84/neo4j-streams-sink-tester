Quickstart
-----------

This package sends record to the Neo4j Kafka Sink by using the following in two data formats:
* JSON, example:
```json
{"name": "Name", "surname": "Surname"}
```

* AVRO, with the schema:

```json
{
 "type":"record",
 "name":"User",
 "fields":[{"name":"name","type":"string"}, {"name":"surname","type":"string"}]
}
```


Before running the tester, make sure that the entire Kafka stack is up-and-running
      
Then run the producer to produce 50 events:

    $ java -jar target/neo4j-streams-sink-tester-1.0.jar -e 50 -Dneo4j.password=<YOUR_PASSWORD>
    
Please type:
    
    $ java -jar target/neo4j-streams-sink-tester-1.0.jar -h

to print the option list.

In order to choose the data format please use the `-f` flag: `-f AVRO` or `-f JSON`.

In addition you can pass the following configuration parameters:
 * -Dkafka.bootstrap.server=<KAFKA_BOOTSTRAP_SERVER,default=localhost:9092>
 * -Dkafka.schema.registry.url=<KAFKA_SCHEMA_REGISTRY_URL,default=http://localhost:8081>
 * -Dkafka.linger.ms=<KAFKA_LINGER_MS,default=0>
 * -Dkafka.batch.size=<KAFKA_BATCH_SIZE,default=16384>

The following parameter are available only in report mode:
 * -Dneo4j.uri=<NEO4_BOLT_URL,default=bolt://localhost:7687>
 * -Dneo4j.user=<NEO4_USERNAME,default=neo4j>
 * -Dneo4j.password=<NEO4_PASSWORD,default=neo4j>