import time
from collections import deque
from threading import Lock, Condition

cdef class FastQueue:
    def __cinit__(self, int maxsize=0):
        self._queue = deque()
        self._lock = Lock()
        self._not_empty = Condition(self._lock)
        self._not_full = Condition(self._lock)
        self._maxsize = maxsize  # 0 = unbounded

    def enqueue(self, object item):
        with self._not_full:
            while self._maxsize and len(self._queue) >= self._maxsize:
                self._not_full.wait()  # block until space is available

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

            item = self._queue.popleft()
            self._not_full.notify()
            return item

    def size(self):
        with self._lock:
            return len(self._queue)

    def empty(self):
        with self._lock:
            return not self._queue