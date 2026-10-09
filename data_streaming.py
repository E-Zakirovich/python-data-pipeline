"""
data_streaming.py
~~~~~~~~~~~~~~~~~

Streams generated data into an in-memory queue (RAM).

Responsibilities:
    - Generate samples
    - Validate samples
    - Push samples to a queue
    - Monitor streaming health
    - Expose runtime statistics
"""

# load packages
import random
from datetime import datetime
import configs as get
from queue import Queue
from pprint import pprint

class DataStreaming:

    def __init__(self):
        self.sample_id : int = 1
        self.running_status : bool = None
        self.queue = Queue()

    def start(self):
        self.running_status = True

    def stop(self):
        self.running_status = False

    def stream(self):
        self.start()
        while self.running_status:
            generated_data = self.generate_sample(self.sample_id)
            if self.validate_sample(generated_data):
                self.sample_id += 1
                self.push_to_queue(generated_data)

            if self.sample_id == 10:
                self.stop()
                self.printer()

            

    def generate_sample(self, id : int):
        data = {
            "id" : id,
            "time" : datetime.now(),
            "feature_one" : random.uniform(get.start, get.end),
            "feature_two" : random.uniform(get.start, get.end),
        }

        return data 

    def validate_sample(self, sample) -> bool:
        return None not in sample.values()

    def push_to_queue(self, sample):
        self.queue.put(sample)

    def printer(self):
        pprint(list(self.queue.queue))

    def get_stats(self):
        pass

    def reset_stats(self):
        pass

    def health_check(self):
        pass


a = DataStreaming()

a.stream()