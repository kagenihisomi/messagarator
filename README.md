# Messagarator: Kafka Data Flow Demo

This repository demonstrates a data pipeline using Kafka, Kafka Connect, and Flink SQL. It includes:

- **Data Generation**: A Python script ([data-generator/data_generator.py](c%3A%2FUsers%2FBrandon%2Fcode%2Fpersonal%2Fmessagarator%2Fdata-generator%2Fdata_generator.py)) produces messages to three Kafka topics:
  - `my-test-topic`: Simple JSON messages.
  - `test-nested`: Messages with nested JSON structures.
  - `flaky-topic`: Messages that can be either valid JSON objects or malformed data, simulating a "flaky" data source.
- **Kafka Connect Sinks**:
  - FileStreamSinkConnector instances ([kafka/connectors/test_sink.json](c%3A%2FUsers%2FBrandon%2Fcode%2Fpersonal%2Fmessagarator%2Fkafka%2Fconnectors%2Ftest_sink.json) and [kafka/connectors/test_sink_nested.json](c%3A%2FUsers%2FBrandon%2Fcode%2Fpersonal%2Fmessagarator%2Fkafka%2Fconnectors%2Ftest_sink_nested.json)) are configured to write data from `my-test-topic` and `test-nested` to local files.
- **Flink SQL Processing**:

  - A Flink SQL script ([flink/process.sql](c%3A%2FUsers%2FBrandon%2Fcode%2Fpersonal%2Fmessagarator%2Fflink%2Fprocess.sql)) consumes messages from `flaky-topic`.
  - Valid JSON objects are written to a file sink (`/tmp/data/flink_output/clean_json_objects/`).
  - Messages that are not valid JSON objects are routed to a Dead Letter Queue (DLQ) Kafka topic (`flaky-topic-dlq`).

- **PostgreSQL Sink**:
  - A Kafka Connect JDBC sink connector writes data from the `test-nested` topic to a PostgreSQL database.
  - The connector requires messages to have schemas (e.g., using Avro, JSON with schemas enabled, or Protobuf).
  - Nested types are flattened before insertion due to PostgreSQL limitations with complex structures.

### Setup stack

```
docker compose -f confluent-stack.yml up -d
```

### Producer

```
poetry install
poetry run python ./data-generator/data_generator.py
```

### Set connectors

```
curl -X PUT -H "Content-Type: application/json" --data @kafka/connectors/test_sink.json http://localhost:8083/connectors/file-sink-connector/config
curl -X DELETE http://localhost:8083/connectors/test_sink_nested

curl -X PUT -H "Content-Type: application/json" --data @kafka/connectors/test_sink_nested.json http://localhost:8083/connectors/test_sink_nested/config

```

### Flaky topic processing

Run Flink

```
docker cp flink/process.sql flink-sql-client:/opt/flink/process_flaky_topic.sql
docker exec -it flink-sql-client /opt/flink/bin/sql-client.sh -f /opt/flink/process_flaky_topic.sql
```

### Postgresql Sink

JDBC sink MUST have schema
https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html#data-mapping

> The sink connector requires knowledge of schemas, so you should use a suitable converter. For example, the Avro converter that comes with Schema Registry, the JSON converter with schemas enabled, or the Protobuf converter.

```json
{
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "test-nested",
    "connection.url": "jdbc:postgresql://postgres:5432/mydatabase",
    "connection.user": "postgres",
    "connection.password": "password",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "event_id",
    "auto.create": "true",
    "auto.evolve": "true",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",

# JDBC sink MUST have schema
# https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html#data-mapping
    "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schemas.enable": "true",

# Nested type doesn't work in postgreSQL
# ConnectException: UserDetails (STRUCT) type doesn't have a mapping to the SQL database column type
    "transforms": "flatten",
    "transforms.flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
    "transforms.flatten.delimiter": "_"
}
```
