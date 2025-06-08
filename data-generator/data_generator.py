import time
import json
import random
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "my-test-topic"
NESTED_TOPIC_NAME = "test-nested"  # New topic for nested data


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

            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
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
    while True:
        # Send to the original topic
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
            # Attempt to reconnect or handle error (simplified for brevity)
            time.sleep(5)
            # Consider re-initializing producer here if connection is lost

        # Send to the nested topic
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
                    round(random.uniform(5.0, 500.0), 2)
                    if random.random() > 0.5
                    else None
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
            # Attempt to reconnect or handle error
            time.sleep(5)

        time.sleep(random.uniform(0.5, 2.0))  # Send messages at random intervals


if __name__ == "__main__":
    main()
