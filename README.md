```
curl -X PUT -H "Content-Type: application/json" --data @kafka/connectors/test_sink.json http://localhost:8083/connectors/file-sink-connector/config


curl -X DELETE http://localhost:8083/connectors/test_sink_nested

curl -X PUT -H "Content-Type: application/json" --data @kafka/connectors/test_sink_nested.json http://localhost:8083/connectors/test_sink_nested/config

```
