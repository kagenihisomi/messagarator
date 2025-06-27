import time
import json
import random

from loguru import logger
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)

from pydantic import BaseModel
from typing import Optional, Dict, Any

KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "my-test-topic"
NESTED_TOPIC_NAME = "test-nested"
FLAKY_TOPIC_NAME = "flaky-topic"


# Define Pydantic models for message schemas
class SimpleMessage(BaseModel):
    id: int
    data: str
    timestamp: float


class UserAttributes(BaseModel):
    is_premium: bool
    country_code: str


class UserDetails(BaseModel):
    user_id: str
    session_id: str
    attributes: UserAttributes


class EventData(BaseModel):
    item_id: Optional[str] = None
    page_url: str
    value: Optional[float] = None


class NestedMessage(BaseModel):
    event_id: str
    event_type: str
    timestamp: float
    user_details: UserDetails
    event_data: EventData
    source_ip: str


class ValidFlakyMessage(BaseModel):
    id: int
    status: str
    data: str
    timestamp: float
    nested_object: Dict[str, Any]


def _delivery_report(err, msg, message_dump):
    """Helper callback for message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Sent to {msg.topic()}: {message_dump}")


class TopicMessageSender:
    """Base class for sending messages to a Kafka topic."""

    def __init__(
        self,
        producer: Producer,
        topic_name: str,
    ):
        """
        Initializes the TopicMessageSender.

        Args:
            producer: The Confluent Kafka Producer instance.
            topic_name: The name of the topic to produce messages to.
        """
        self.producer = producer
        self.topic_name = topic_name
        self._message_id = 0

    def send(self) -> None:
        """Sends a message to the topic. Must be implemented by subclasses."""
        raise NotImplementedError


class PydanticMessageSender(TopicMessageSender):
    """A message sender for Pydantic models using Schema Registry."""

    def __init__(
        self,
        producer: Producer,
        topic_name: str,
        pydantic_model: type[BaseModel],
        schema_registry_client: SchemaRegistryClient,
    ):
        """
        Initializes the PydanticMessageSender.

        Args:
            producer: The Confluent Kafka Producer instance.
            topic_name: The name of the topic.
            pydantic_model: The Pydantic model class for the message schema.
            schema_registry_client: The Schema Registry client.
        """
        super().__init__(producer, topic_name)
        self.pydantic_model = pydantic_model
        schema = pydantic_model.model_json_schema()
        logger.info(
            f"Schema for {pydantic_model.__name__} on topic '{topic_name}':\n{json.dumps(schema, indent=2)}"
        )
        self._serializer = JSONSerializer(
            schema_str=json.dumps(schema),
            schema_registry_client=schema_registry_client,
            to_dict=lambda obj, ctx: obj.model_dump(),
        )

    def _create_message(self) -> BaseModel:
        """Creates a Pydantic message object. Must be implemented by subclasses."""
        raise NotImplementedError

    def send(self) -> None:
        """Creates, serializes, and sends a Pydantic message."""
        try:
            message = self._create_message()
            # Use a common attribute for key if available, otherwise use message id
            key = str(
                getattr(message, "id", getattr(message, "event_id", self._message_id))
            )
            self.producer.produce(
                topic=self.topic_name,
                key=key,
                value=self._serializer(
                    message, SerializationContext(self.topic_name, MessageField.VALUE)
                ),
                on_delivery=lambda err, msg: _delivery_report(
                    err, msg, message.model_dump()
                ),
            )
            self._message_id += 1
        except Exception as e:
            logger.error(
                f"Error sending message to {self.topic_name}: {e}", exc_info=True
            )
            time.sleep(5)


class SimpleMessageSender(PydanticMessageSender):
    """Sends SimpleMessage objects to the simple topic."""

    def __init__(
        self, producer: Producer, schema_registry_client: SchemaRegistryClient
    ):
        super().__init__(producer, TOPIC_NAME, SimpleMessage, schema_registry_client)

    def _create_message(self) -> SimpleMessage:
        """Creates a new SimpleMessage."""
        return SimpleMessage(
            id=self._message_id,
            data=f"Sample data point {random.randint(1, 100)}",
            timestamp=time.time(),
        )


class NestedMessageSender(PydanticMessageSender):
    """Sends NestedMessage objects to the nested topic."""

    def __init__(
        self, producer: Producer, schema_registry_client: SchemaRegistryClient
    ):
        super().__init__(
            producer, NESTED_TOPIC_NAME, NestedMessage, schema_registry_client
        )

    def _create_message(self) -> NestedMessage:
        """Creates a new NestedMessage."""
        return NestedMessage(
            event_id=f"event_{self._message_id}",
            event_type=random.choice(["user_login", "item_viewed", "order_placed"]),
            timestamp=time.time(),
            user_details=UserDetails(
                user_id=f"user_{random.randint(1000, 2000)}",
                session_id=f"session_{random.getrandbits(32)}",
                attributes=UserAttributes(
                    is_premium=random.choice([True, False]),
                    country_code=random.choice(["US", "CA", "GB", "DE"]),
                ),
            ),
            event_data=EventData(
                item_id=(
                    f"item_{random.randint(1, 500)}" if random.random() > 0.3 else None
                ),
                page_url=f"/page/{random.randint(1,10)}.html",
                value=(
                    round(random.uniform(5.0, 500.0), 2)
                    if random.random() > 0.5
                    else None
                ),
            ),
            source_ip=f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
        )


class FlakyMessageSender(TopicMessageSender):
    """
    Sends messages to the flaky topic.

    With a 10% chance, it sends a malformed or raw string message.
    Otherwise, it sends a valid, schema-registered message if a
    schema registry client is provided.
    """

    def __init__(
        self,
        producer: Producer,
        schema_registry_client: Optional[SchemaRegistryClient] = None,
    ):
        """
        Initializes the FlakyMessageSender.

        Args:
            producer: The Confluent Kafka Producer instance.
            schema_registry_client: An optional Schema Registry client. If provided,
                                    it's used to serialize valid messages.
        """
        super().__init__(producer, FLAKY_TOPIC_NAME)
        self._serializer = None
        if schema_registry_client:
            self._serializer = JSONSerializer(
                schema_str=json.dumps(ValidFlakyMessage.model_json_schema()),
                schema_registry_client=schema_registry_client,
                to_dict=lambda obj, ctx: obj.model_dump(),
            )

    def send(self) -> None:
        """Sends either a valid message or a malformed/raw string."""
        key = str(self._message_id)
        try:
            # If we have a serializer and we are not in the 10% failure case, send valid message
            if self._serializer and random.random() >= 0.1:
                valid_message = ValidFlakyMessage(
                    id=self._message_id,
                    status="valid",
                    data=f"Some good data {random.randint(1, 100)}",
                    timestamp=time.time(),
                    nested_object={"key1": "value1", "key2": random.random()},
                )
                value = self._serializer(
                    valid_message,
                    SerializationContext(self.topic_name, MessageField.VALUE),
                )
                self.producer.produce(
                    topic=self.topic_name,
                    key=key,
                    value=value,
                    on_delivery=lambda err, msg: _delivery_report(
                        err, msg, valid_message.model_dump()
                    ),
                )
            else:  # 10% chance of invalid JSON, or always if no serializer
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
                    payload_str = (
                        f"This is some raw, non-JSON text. ID: {self._message_id}"
                    )
                elif malformed_type == "missing_closing_brace":
                    payload_str = f'{{"id": {self._message_id}, "description": "This JSON is missing a closing brace", "value": {random.randint(1,100)}'
                elif malformed_type == "unquoted_key":
                    payload_str = (
                        f'{{id: {self._message_id}, "error": "key not quoted"}}'
                    )
                elif malformed_type == "trailing_comma":
                    payload_str = f'{{"id": {self._message_id}, "data": "value",}}'
                elif malformed_type == "just_a_string_not_json_object":
                    payload_str = '"This is a valid JSON string, but perhaps not what a consumer expecting an object wants."'

                payload_bytes = payload_str.encode("utf-8")
                self.producer.produce(
                    topic=self.topic_name,
                    key=key,
                    value=payload_bytes,
                    on_delivery=lambda err, msg: _delivery_report(
                        err, msg, payload_str
                    ),
                )
            self._message_id += 1
        except Exception as e:
            logger.error(
                f"Error sending message to {self.topic_name}: {e}",
                exc_info=True,
            )
            time.sleep(5)


def create_topic_if_not_exists(admin_client, topic_name):
    """Creates a topic using confluent_kafka AdminClient if it doesn't exist."""
    try:
        cluster_metadata = admin_client.list_topics(timeout=5)
        if topic_name in cluster_metadata.topics:
            logger.info(f"Topic '{topic_name}' already exists.")
            return

        logger.info(f"Topic '{topic_name}' not found, attempting to create.")
        topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
        futures = admin_client.create_topics([topic])
        for topic, future in futures.items():
            try:
                future.result()  # The result itself is None on success
                logger.info(f"Topic '{topic}' created successfully.")
            except Exception as e:
                # Handle race condition where topic is created by another process
                if e.args[0].code() == 40:  # TOPIC_ALREADY_EXISTS
                    logger.info(
                        f"Topic '{topic}' already exists (created concurrently)."
                    )
                else:
                    logger.error(f"Failed to create topic '{topic}': {e}")
                    raise
    except Exception as e:
        logger.error(f"An error occurred during topic creation: {e}")
        time.sleep(5)
        raise


def main():
    producer: Optional[Producer] = None
    admin: Optional[AdminClient] = None
    retries = 10
    for i in range(retries):
        try:
            logger.info(f"Attempting to connect to Kafka ({i+1}/{retries})...")
            admin_config = {"bootstrap.servers": KAFKA_BROKER}
            admin = AdminClient(admin_config)
            admin.list_topics(timeout=5)
            logger.success("Successfully connected to Kafka.")

            create_topic_if_not_exists(admin, TOPIC_NAME)
            create_topic_if_not_exists(admin, NESTED_TOPIC_NAME)
            create_topic_if_not_exists(admin, FLAKY_TOPIC_NAME)

            schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)

            producer_conf = {"bootstrap.servers": KAFKA_BROKER}
            producer = Producer(producer_conf)

            logger.success("Schema Registry and Producer are ready.")
            break
        except Exception as e:
            logger.warning(
                f"Kafka not ready yet or setup failed: {e}. Retrying in 5 seconds..."
            )
            time.sleep(5)
            if i == retries - 1:
                logger.critical(
                    "Failed to connect to Kafka after multiple retries. Exiting."
                )
                return

    if not producer:
        return

    simple_sender = SimpleMessageSender(producer, schema_registry_client)
    nested_sender = NestedMessageSender(producer, schema_registry_client)
    flaky_sender = FlakyMessageSender(producer, schema_registry_client)

    while True:
        simple_sender.send()

        nested_sender.send()
        flaky_sender.send()
        producer.poll(0)
        producer.flush()
        time.sleep(random.uniform(0.5, 2.0))


if __name__ == "__main__":
    main()
