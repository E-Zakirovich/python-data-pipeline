"""
multi_thread.py
~~~~~~~~~~~~~~~

i dunno what to say, just a comment
"""

import threading
from queue import Queue, Empty

class MultiThread:
    def __init__(self, input_quee, num_workers = 4):
        self.input_queue = input_quee
        self.results = Queue()
        self.num_workers = num_workers
        self.stop_event = threading.Event()
        self.threads = []

    def preprocessing(self, matrix):
        return matrix.mean(axis = 0)

    def worker(self):
        while not self.stop_event.is_set():
            try:
                matrix = self.input_queue.get(timeout=1)
            except Empty:
                continue
            self.results.put(self.preprocessing(matrix))

    def start(self):
        for _ in range(self.num_workers):
            t = threading.Thread(target=self.worker, daemon=True)
            t.start()
            self.threads.append(t)

    def stop(self):
        self.stop_event.set()
        for t in self.threads:
            t.join()