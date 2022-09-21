"""
A basic example of a Redpanda consumer
"""
import signal
from dataclasses import dataclass
from time import sleep

from kafka import KafkaConsumer

is_shutting_down = False

@dataclass
class ConsumerConfig:
    bootstrap_servers = "localhost"
    topic = "streaming-pipeline"
    consumer_group = "sample_group"
    auto_offset_reset = "latest"
    group_id = "group1"

def process_message(msg):
        print(f"processing task....")
        sleep(1)
        print(f"processing next task....")
        sleep(1)
        print(f"task {msg} finish\n")

def graceful_exit(*arg, **kwargs):
    global is_shutting_down
    is_shutting_down = True

   


class Consumer:
    def __init__(self, config: ConsumerConfig):
        self.client = KafkaConsumer(
            config.topic,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.consumer_group,
            auto_offset_reset=config.auto_offset_reset,
            # add more configs here if you'd like
        )
        self.topic = config.topic
    
    def consume(self):
        """Consume messages from a Redpanda topic"""
        print("starting streaming consumer app")
        try:
            for msg in self.client:
                process_message(msg.value)
                if is_shutting_down:
                    break
            
                # print(f"Consumed record. key={msg.key}, value={msg.value}")
            print("End of the program. it was killed gracefully")
            self.client.close()
        except:
            print(f"Could not consume from topic: {self.topic}")
            raise

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

config = ConsumerConfig()
redpanda_consumer = Consumer(config)
redpanda_consumer.consume()
