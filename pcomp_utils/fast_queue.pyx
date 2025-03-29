from collections import deque
from threading import Lock, Condition
from queue import Empty  # Import Empty exception for timeout handling

cdef class FastQueue:
    cdef object _queue
    cdef object _lock
    cdef object _not_empty
    cdef object _not_full
    cdef int _maxsize

    def __cinit__(self, int maxsize=0):
        self._queue = deque()
        self._lock = Lock()
        self._not_empty = Condition(self._lock)
        self._not_full = Condition(self._lock)
        self._maxsize = maxsize

    def put(self, object item, double timeout=-1):
        with self._not_full:
            while self._maxsize and len(self._queue) >= self._maxsize:
                if timeout < 0:
                    self._not_full.wait()  # wait indefinitely
                else:
                    if not self._not_full.wait(timeout):
                        raise Exception("put timeout")
            self._queue.append(item)
            self._not_empty.notify()

    def get(self, double timeout=-1):
        with self._not_empty:
            while not self._queue:
                if timeout < 0:
                    self._not_empty.wait()  # wait indefinitely
                else:
                    if not self._not_empty.wait(timeout):
                        raise Empty
            item = self._queue.popleft()
            self._not_full.notify()
            return item

    def size(self):
        with self._lock:
            return len(self._queue)

    def empty(self):
        with self._lock:
            return not self._queue
