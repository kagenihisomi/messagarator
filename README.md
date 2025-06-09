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
