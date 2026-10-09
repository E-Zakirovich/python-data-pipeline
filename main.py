import threading
import multiprocessing as mp

from data_streaming import DataStreaming
from data_batching import DataBatching
from parallel_processing import ParallelProcessing


if __name__ == "__main__":
    streamer = DataStreaming()
    batcher = DataBatching(streamer)               # batcher.output must be mp.Queue()
    p_processing = ParallelProcessing(batcher.output, num_workers=4)

    threading.Thread(target=streamer.stream, daemon=True).start()
    threading.Thread(target=batcher.run, daemon=True).start()
    p_processing.start()

    for _ in range(10):
        print(p_processing.results.get())

    streamer.stop()
    batcher.stop()
    p_processing.stop()