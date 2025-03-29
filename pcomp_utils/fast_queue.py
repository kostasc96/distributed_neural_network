import threading
from collections import deque
import time

class FastQueue:
    def __init__(self):
        self._queue = deque()
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)

    def enqueue(self, item):
        with self._not_empty:
            self._queue.append(item)
            self._not_empty.notify()

    def dequeue(self, timeout=None):
        with self._not_empty:
            if not self._queue:
                waited = self._not_empty.wait(timeout)
                if not waited:
                    return None
            if self._queue:
                return self._queue.popleft()
            return None

    def size(self):
        with self._lock:
            return len(self._queue)

    def empty(self):
        with self._lock:
            return not self._queue
