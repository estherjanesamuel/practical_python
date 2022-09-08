# Building Realtime Streaming & Batch Workers Using Python & Kafka or Redpanda

## Worker
What it is? Program that always listening on an event in a system and perform some actions in the background asynchronously.

### Streaming Worker
process each incoming event or message immediately

### Bath Worker
accumulate the event or message into a buffer queue then process whena particular condition has met.

## Graceful Shutdown
 What it is ? It's to how the service handle incoming signal or cancelation token to finish all running task and stop pulling for new event or terminated the rest with gracefulness, see Disposability rule in Twelve Factor App.  
 `note: use to notify the program that you want to bring down the service e.g: CTRL + C`



## Usecases
- Welcome email after registration 
- Updating user record from status 'Pending' to 'Waiting Confirmation'


## POC
- Building minimal workers to understand how both of the work.

Producer - Message Queue or Broker - Consumer


## Objective or Goal
- Discuss on how streaming and batch worker works behinds the scene.
- Hands On

## Hands On

## Environment Setup

### Python / Dotnet / Go
### Redpanda / Kafka (Docker)
#### Worker

producer.py

```py
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
    
```  

    
    
- Graceful Shutdown
    - asads

## Conclusion / Key Takeaways
Two Essential concepts in this tutorial:
- Straming & Batch Worker
- Graceful shutdown

These concept are language agnostic and broker agnostic, that apply to all programming languages and brokers.

I hope you learn something from this post. Would you please support me in writing more tutorial ?

## References







