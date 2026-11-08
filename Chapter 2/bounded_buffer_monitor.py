"""
Parallel & Distributed Computing - Assignment 2
Theme : Coordinating concurrent threads with counting semaphores and a mutex
Basis : Chapter 2 (Thread-based Parallelism) - Python Parallel Programming Cookbook

What this version does differently from the textbook recipe:
  - The textbook relies on plain sleeps and an unbounded list. Here the shared
    store is wrapped inside a single class so that every access goes through one
    well-defined gateway (encapsulation).
  - Two counting semaphores track free vs. occupied positions, so writers stall
    on a full store and readers stall on an empty one - no busy spinning.
  - A mutex guards the deque mutations so the critical section stays atomic.
  - A threading.Event lets the main thread tell the readers to drain and quit
    without hanging, and every operation is traced with a millisecond timestamp.
"""

import threading
import time
import random
from collections import deque
from datetime import datetime

# ---- Tunable parameters ------------------------------------------------------
CAPACITY = 8        # how many payloads the store can hold at once
WORKLOAD = 24       # total number of payloads produced across all writers


def trace(tag: str, state: str, note: str) -> None:
    """Print one millisecond-stamped line describing a thread event."""
    stamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"{stamp}  |{tag:^12}|  {state:<14}  ->  {note}")


class BoundedStore:
    """
    A fixed-capacity, thread-safe container.

    All concurrency control is kept here so the worker functions stay simple:
      * _vacant   - counts positions still free (writers wait on it)
      * _ready    - counts positions holding data (readers wait on it)
      * _gate     - mutex protecting the underlying deque
      * _halt     - event flipped on when no more data will ever arrive
    """

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._queue = deque()
        self._gate = threading.Lock()
        self._vacant = threading.Semaphore(capacity)
        self._ready = threading.Semaphore(0)
        self._halt = threading.Event()

    def put(self, payload: str, who: str) -> None:
        """Block until a slot is free, then append the payload atomically."""
        self._vacant.acquire()
        with self._gate:
            self._queue.append(payload)
            trace(who, "ENQUEUED", f"{payload}  (load {len(self._queue)}/{self.capacity})")
        self._ready.release()

    def take(self, who: str):
        """Try to remove one payload. Returns None on timeout or empty store."""
        if not self._ready.acquire(timeout=0.5):
            return None

        payload = None
        with self._gate:
            if self._queue:
                payload = self._queue.popleft()
                trace(who, "DEQUEUED", f"{payload}  (load {len(self._queue)}/{self.capacity})")

        if payload is not None:
            self._vacant.release()
        return payload

    def stop(self) -> None:
        """Announce that production is over so readers can wind down."""
        self._halt.set()

    @property
    def closing(self) -> bool:
        return self._halt.is_set()

    def __len__(self) -> int:
        return len(self._queue)


def writer(store: BoundedStore, wid: int, quota: int) -> None:
    """Generate `quota` payloads and hand them to the shared store."""
    tag = f"Writer-{wid}"
    trace(tag, "ONLINE", "worker thread started")

    for n in range(quota):
        payload = f"W{wid}:item-{n + 1}"
        time.sleep(random.uniform(0.1, 0.4))   # pretend to do some work first
        store.put(payload, tag)

    trace(tag, "RETIRED", "finished its quota")


def reader(store: BoundedStore, rid: int) -> None:
    """Pull payloads out of the store until it is both closing and empty."""
    tag = f"Reader-{rid}"
    trace(tag, "ONLINE", "worker thread started")

    while not store.closing or len(store) > 0:
        payload = store.take(tag)
        if payload is None:
            continue
        time.sleep(random.uniform(0.2, 0.5))   # pretend to process the payload

    trace(tag, "RETIRED", "drained and exited")


def main() -> None:
    bar = "-" * 78
    print(bar)
    print("  PDC ASSIGNMENT 2  :  Bounded-Store Writer / Reader Coordination")
    print(f"  Store capacity = {CAPACITY}   |   Total payloads = {WORKLOAD}")
    print(bar)

    began = time.time()
    store = BoundedStore(CAPACITY)

    writer_count = 4
    reader_count = 3
    quota_each = WORKLOAD // writer_count

    writers = [
        threading.Thread(target=writer, args=(store, i + 1, quota_each), name=f"Writer-{i + 1}")
        for i in range(writer_count)
    ]
    readers = [
        threading.Thread(target=reader, args=(store, i + 1), name=f"Reader-{i + 1}")
        for i in range(reader_count)
    ]

    # Readers come up first so they are already waiting when data appears.
    for r in readers:
        r.start()
    for w in writers:
        w.start()

    # Once every writer has emptied its quota, tell the readers to wind down.
    for w in writers:
        w.join()
    store.stop()

    for r in readers:
        r.join()

    print(bar)
    print(f"  Wall-clock time : {time.time() - began:.2f} s")
    print(f"  Leftover in store : {len(store)} payload(s)")
    print(bar)


if __name__ == "__main__":
    main()
