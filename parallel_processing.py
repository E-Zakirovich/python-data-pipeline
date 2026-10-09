import multiprocessing as mp
from queue import Empty


def process(matrix):
    # same placeholder as the thread version
    return matrix.mean(axis=0)


def _worker(input_queue, results, stop_event):
    while not stop_event.is_set():
        try:
            matrix = input_queue.get(timeout=1)
        except Empty:
            continue
        results.put(process(matrix))


class ParallelProcessing:

    def __init__(self, input_queue, num_workers: int = 4):
        self.input_queue = input_queue
        self.results = mp.Queue()
        self.num_workers = num_workers
        self.stop_event = mp.Event()
        self.processes = []

    def start(self):
        for _ in range(self.num_workers):
            p = mp.Process(
                target=_worker,
                args=(self.input_queue, self.results, self.stop_event),
                daemon=True,
            )
            p.start()
            self.processes.append(p)

    def stop(self):
        self.stop_event.set()
        for p in self.processes:
            p.join()