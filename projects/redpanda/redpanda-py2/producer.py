"""
A basic example of a Redpanda producer
"""
import os
from dataclasses import dataclass

from kafka import KafkaProducer


@dataclass
class ProducerConfig:
    """a Dataclass for storing our producer configs"""

    # the Redpanda bootstrap servers will be pulled from the
    # REDPANDA_BROKERS environment variable. Ensure this is set
    # before running the code
    bootstrap_servers = "localhost"
    topic = "streaming-pipeline"
    consumer_group = "sample_group"
    auto_offset_reset = "latest"


class Producer:
    def __init__(self, config: ProducerConfig):
        # instantiate a Kafka producer client. This client is compatible
        # with Redpanda brokers
        self.client = KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,
            # add more configs here if you'd like
            key_serializer=str.encode,
            value_serializer=str.encode,
        )
        self.topic = config.topic

    def produce(self, message: str, key: str):
        """Produce a single message to a Redpanda topic"""
        try:
            # send a message to the topic
            future = self.client.send(self.topic, key=key, value=message)

            # this line will block until the message is sent (or timeout).
            _ = future.get(timeout=10)
            print(f"Successfully produced message to topic: {self.topic}")
        except:
            print(f"Could not produce to topic: {self.topic}")
            raise


# create a config and producer instance
config = ProducerConfig()
redpanda_producer = Producer(config)

for i in range(10):
    # produce a greeting to the topic
    redpanda_producer.produce( str(i),"{\"pid\":1%s%s,\"recommended_pids\":[%s%s%s,789]})" %(str(i), str(i + 1),str(i + 1),str(i + 2),str(i + 3)))
    