import time
import json
import random
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer

KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "my-test-topic"


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
    while True:
        message = {
            "id": message_id,
            "data": f"Sample data point {random.randint(1, 100)}",
            "timestamp": time.time(),
        }
        try:
            producer.send(TOPIC_NAME, value=message)
            print(f"Sent: {message}")
            message_id += 1
            time.sleep(random.uniform(0.5, 2.0))  # Send messages at random intervals
        except Exception as e:
            print(f"Error sending message: {e}")
            # Attempt to reconnect or handle error
            time.sleep(5)
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
            except Exception as recon_e:
                print(f"Failed to reconnect producer: {recon_e}")
                break  # Exit loop if reconnect fails


if __name__ == "__main__":
    main()
