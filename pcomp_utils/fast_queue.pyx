import time
from collections import deque
from threading import Lock, Condition

cdef class FastQueue:
    def __cinit__(self):
        self._queue = deque()
        self._lock = Lock()
        self._not_empty = Condition(self._lock)

    def enqueue(self, object item):
        with self._not_empty:
            self._queue.append(item)
            self._not_empty.notify()

    def dequeue(self, double timeout=0.0):
        cdef double deadline = time.time() + timeout if timeout > 0 else 0.0
        with self._not_empty:
            while not self._queue:
                remaining = deadline - time.time() if timeout > 0 else None
                if timeout > 0 and remaining <= 0:
                    return None
                self._not_empty.wait(timeout=remaining)
            return self._queue.popleft()

    def size(self):
        with self._lock:
            return len(self._queue)

    def empty(self):
        with self._lock:
            return not self._queue
