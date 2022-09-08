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
To Simulate produce an event or message 

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

consumer.py  
consume an event
```py
"""
A basic example of a Redpanda consumer
"""
import os
from dataclasses import dataclass
from time import sleep

from kafka import KafkaConsumer


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
                # print(f"Consumed record. key={msg.key}, value={msg.value}")
        except:
            print(f"Could not consume from topic: {self.topic}")
            raise
    

config = ConsumerConfig()
redpanda_consumer = Consumer(config)
redpanda_consumer.consume()
``` 
    
- Graceful Shutdown
```py
"""
A basic example of a Redpanda consumer
"""
import os
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
```

consumer_batch.py
```py
"""
A basic example of a Redpanda consumer
"""
import signal
import threading
from dataclasses import dataclass
from time import sleep
from kafka import KafkaConsumer
from queue import Queue, Empty

tasks = Queue()

is_shutting_down = False

@dataclass
class ConsumerConfig:
    bootstrap_servers = "localhost"
    topic = "streaming-pipeline"
    consumer_group = "sample_group"
    auto_offset_reset = "latest"
    group_id = "group1"


def process_message():
    print("processing task in queue buffer....")
    
    temp_task = []
    try:
        while True:
            temp_task.append(tasks.get_nowait())
    except:
        pass

    # combine all tasks in 1 call, this is the beuty of batch worker
    print("processing task...." + str(temp_task))
    sleep(0.5)
    print("\nprocessing next task...." + str(temp_task))
    sleep(0.5)
    print(f"\ntask finish... \n" )


def graceful_exit(*args, **kwargs):
    global is_shutting_down
    is_shutting_down = True
    process_message()
    exit() 


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        
    def run(self): 
        consumer = KafkaConsumer(
            "streaming-pipeline", bootstrap_servers=["localhost:9092"], group_id="group1"
        )
        try:
            for msg in consumer:
                self.insert_into_buffer(msg.value)
                
                if is_shutting_down:
                    break
                # print(f"Consumed record. key={msg.key}, value={msg.value}")
            consumer.close()
        except:
            print(f"Could not consume from topic: {self.topic}")
            raise
    
    def insert_into_buffer(self, msg: str):
        # Insert the task/message into buffer que
        print("receive a message, inserting into a queue buffer")
        tasks.put(msg)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)
 
    redpanda_consumer = Consumer()
    redpanda_consumer.daemon = True
    redpanda_consumer.start()
    
    while True:
        process_message()
        sleep(5)

            # print("End of the program. it was killed gracefully")

```

## Conclusion / Key Takeaways
Two Essential concepts in this tutorial:
- Straming & Batch Worker
- Graceful shutdown

These concept are language agnostic and broker agnostic, that apply to all programming languages and brokers.

I hope you learn something from this post. Would you please support me in writing more tutorial ?

## References







