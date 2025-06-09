import time
import json
import random
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "my-test-topic"
NESTED_TOPIC_NAME = "test-nested"  # New topic for nested data
FLAKY_TOPIC_NAME = "flaky-topic"  # New topic for potentially invalid JSON


def custom_value_serializer(v):
    if isinstance(v, bytes):
        return v  # If it's already bytes, pass it through
    return json.dumps(v).encode("utf-8")  # Otherwise, dump to JSON and encode


def create_topic_if_not_exists(admin_client, topic_name):
    try:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")
        # Potentially retry or exit if critical
        time.sleep(5)  # Wait before retrying or if Kafka is not ready
        raise


def send_to_topic_name(producer, message_id):
    message = {
        "id": message_id,
        "data": f"Sample data point {random.randint(1, 100)}",
        "timestamp": time.time(),
    }
    try:
        producer.send(TOPIC_NAME, value=message)
        print(f"Sent to {TOPIC_NAME}: {message}")
        message_id += 1
    except Exception as e:
        print(f"Error sending message to {TOPIC_NAME}: {e}")
        time.sleep(5)
    return message_id


def send_to_nested_topic(producer, nested_message_id):
    nested_message = {
        "event_id": f"event_{nested_message_id}",
        "event_type": random.choice(["user_login", "item_viewed", "order_placed"]),
        "timestamp": time.time(),
        "user_details": {
            "user_id": f"user_{random.randint(1000, 2000)}",
            "session_id": f"session_{random.getrandbits(32)}",
            "attributes": {
                "is_premium": random.choice([True, False]),
                "country_code": random.choice(["US", "CA", "GB", "DE"]),
            },
        },
        "event_data": {
            "item_id": (
                f"item_{random.randint(1, 500)}" if random.random() > 0.3 else None
            ),
            "page_url": f"/page/{random.randint(1,10)}.html",
            "value": (
                round(random.uniform(5.0, 500.0), 2) if random.random() > 0.5 else None
            ),
        },
        "source_ip": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
    }
    try:
        producer.send(NESTED_TOPIC_NAME, value=nested_message)
        print(f"Sent to {NESTED_TOPIC_NAME}: {nested_message}")
        nested_message_id += 1
    except Exception as e:
        print(f"Error sending message to {NESTED_TOPIC_NAME}: {e}")
        time.sleep(5)
    return nested_message_id


def send_to_flaky_topic(producer, flaky_message_id):
    try:
        if random.random() < 0.1:  # 10% chance of invalid JSON
            malformed_type = random.choice(
                [
                    "raw_text",
                    "missing_closing_brace",
                    "unquoted_key",
                    "trailing_comma",
                    "just_a_string_not_json_object",
                ]
            )
            payload_str = ""
            if malformed_type == "raw_text":
                payload_str = f"This is some raw, non-JSON text. ID: {flaky_message_id}"
            elif malformed_type == "missing_closing_brace":
                payload_str = f'{{"id": {flaky_message_id}, "description": "This JSON is missing a closing brace", "value": {random.randint(1,100)}'
            elif malformed_type == "unquoted_key":
                payload_str = f'{{id: {flaky_message_id}, "error": "key not quoted"}}'
            elif malformed_type == "trailing_comma":
                payload_str = f'{{"id": {flaky_message_id}, "data": "value",}}'
            elif malformed_type == "just_a_string_not_json_object":
                payload_str = '"This is a valid JSON string, but perhaps not what a consumer expecting an object wants."'

            payload_bytes = payload_str.encode("utf-8")
            producer.send(FLAKY_TOPIC_NAME, value=payload_bytes)
            print(
                f"Sent INVALID to {FLAKY_TOPIC_NAME} (type: {malformed_type}): {payload_str}"
            )
        else:
            payload_dict = {
                "id": flaky_message_id,
                "status": "valid",
                "data": f"Some good data {random.randint(1, 100)}",
                "timestamp": time.time(),
                "nested_object": {"key1": "value1", "key2": random.random()},
            }
            producer.send(FLAKY_TOPIC_NAME, value=payload_dict)
            print(f"Sent VALID to {FLAKY_TOPIC_NAME}: {payload_dict}")
        flaky_message_id += 1
    except Exception as e:
        print(f"Error sending message to {FLAKY_TOPIC_NAME}: {e}")
        time.sleep(5)
    return flaky_message_id


def main():
    # Wait for Kafka to be available
    producer = None
    admin = None
    retries = 10
    for i in range(retries):
        try:
            print(f"Attempting to connect to Kafka ({i+1}/{retries})...")
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            create_topic_if_not_exists(admin, TOPIC_NAME)
            create_topic_if_not_exists(admin, NESTED_TOPIC_NAME)  # Create the new topic
            create_topic_if_not_exists(
                admin, FLAKY_TOPIC_NAME
            )  # Create the flaky topic

            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=custom_value_serializer,  # Use the custom serializer
            )
            print("Successfully connected to Kafka and topic is ready.")
            break
        except Exception as e:
            print(
                f"Kafka not ready yet or topic creation failed: {e}. Retrying in 5 seconds..."
            )
            time.sleep(5)
            if i == retries - 1:
                print("Failed to connect to Kafka after multiple retries. Exiting.")
                return  # Exit if connection fails after retries

    if not producer:
        return

    message_id = 0
    nested_message_id = 0
    flaky_message_id = 0  # Initialize ID for flaky messages
    while True:
        message_id = send_to_topic_name(producer, message_id)
        nested_message_id = send_to_nested_topic(producer, nested_message_id)
        flaky_message_id = send_to_flaky_topic(producer, flaky_message_id)

        time.sleep(random.uniform(0.5, 2.0))  # Send messages at random intervals


if __name__ == "__main__":
    main()
