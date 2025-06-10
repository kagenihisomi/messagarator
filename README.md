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
