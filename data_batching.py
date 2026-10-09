"""
data_batching.py
~~~~~~~~~~~~~~~~

I need this file for for batch the data
"""

from data_streaming import DataStreaming
import numpy as np
from queue import Queue, Empty
import multiprocessing as mp


data = DataStreaming()

class DataBatching:

    def __init__(self, streamer, batch_size: int = 1000):
        self.streamer = streamer
        self.batch_size = batch_size
        self.output = mp.Queue()        # full matrices wait here for workers
        self.running = False

    def run(self):
        self.running = True
        batch = []
        while self.running:
            try:
                s = self.streamer.get_sample(timeout=1)
            except Empty:
                continue
            batch.append([s["feature_one"], s["feature_two"]])
            if len(batch) == self.batch_size:
                self.output.put(np.array(batch, dtype=np.float32))
                batch = []

    def stop(self):
        self.running = False