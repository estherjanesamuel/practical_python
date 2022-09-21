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
    print("processing next task...." + str(temp_task))
    sleep(0.5)
    print(f"task finish... \n" )


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
