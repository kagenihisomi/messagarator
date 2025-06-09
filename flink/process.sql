CREATE TABLE
    IF NOT EXISTS FlakyTopicSource (`raw_message_value` STRING)
WITH
    (
        'connector' = 'kafka',
        'topic' = 'flaky-topic', -- Source topic
        'properties.bootstrap.servers' = 'broker:29092',
        'properties.group.id' = 'flink-flaky-source-to-file-group', -- New group ID
        'format' = 'raw',
        'scan.startup.mode' = 'earliest-offset'
    );

-- Sink for valid JSON OBJECTS, writing directly to a local filesystem as JSONL
CREATE TABLE
    IF NOT EXISTS CleanJsonObjectsFileSink (
        `json_string_value` STRING -- This will hold the JSON object string
    )
WITH
    (
        'connector' = 'filesystem',
        'path' = 'file:///tmp/data/flink_output/clean_json_objects/', -- Output directory inside TaskManager containers
        'format' = 'raw', -- Writes the content of json_string_value directly, one per line
        -- Since each string is a JSON object, this creates JSONL.
        -- Optional: Rolling policy for file creation
        'sink.rolling-policy.file-size' = '10MB', -- Roll to a new file every 10MB
        'sink.rolling-policy.rollover-interval' = '1m' -- Or every 10 minutes
    );

-- Sink for messages that are NOT JSON OBJECTS (DLQ), writing to a Kafka topic
CREATE TABLE
    IF NOT EXISTS NonJsonObjectDlqKafkaSink ( -- Renamed for clarity
        `original_raw_value` STRING
    )
WITH
    (
        'connector' = 'kafka',
        'topic' = 'flaky-topic-dlq', -- DLQ Kafka topic
        'properties.bootstrap.servers' = 'broker:29092',
        'format' = 'raw'
    );

BEGIN STATEMENT
SET;

-- Process messages that are valid JSON OBJECTS and write to file
INSERT INTO
    CleanJsonObjectsFileSink
SELECT
    `raw_message_value`
FROM
    FlakyTopicSource
WHERE
    `raw_message_value` IS JSON OBJECT;

-- Process messages that are NOT valid JSON OBJECTS and write to DLQ Kafka topic
INSERT INTO
    NonJsonObjectDlqKafkaSink
SELECT
    `raw_message_value`
FROM
    FlakyTopicSource
WHERE
    `raw_message_value` IS NOT JSON OBJECT;

END;