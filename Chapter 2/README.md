# Parallel & Distributed Computing — Assignment 2
**Theme:** Thread-based Parallelism & Synchronization (Chapter 2)
**Program:** A bounded writer/reader pipeline built around counting semaphores and a mutex

---

## 1. What this is
A coursework submission for the Parallel & Distributed Computing module. It builds on the
thread-synchronization ideas from **Chapter 2** of the *Python Parallel Programming Cookbook
(Second Edition)*, Packt Publishing.

The book's recipe only shows the bare bones — a `threading.Semaphore` together with an
ever-growing shared list. This program reworks that idea into a proper **bounded store** where
a group of writer threads and reader threads hand data back and forth safely, with no overflow,
no underflow, and no race conditions on the shared container.

---

## 2. Design choices
1. **One object owns the concurrency.**
   All of the locking and signalling lives inside a single `BoundedStore` class, so the writer
   and reader functions never touch the semaphores directly. This keeps the critical section in
   exactly one place.
2. **Two counting semaphores instead of sleeps.**
   - `_vacant` starts at `CAPACITY (8)` and tracks free positions, so writers block the moment
     the store is full.
   - `_ready` starts at `0` and tracks filled positions, so readers block the moment the store
     is empty.
3. **Mutex around every mutation.**
   A `threading.Lock` wraps each `append` / `popleft` on the internal `deque`, guaranteeing the
   buffer update is atomic.
4. **Clean shutdown.**
   A `threading.Event` plus a half-second timed `acquire` lets readers finish any remaining data
   and then exit on their own, with no deadlocks or hung threads.
5. **Readable tracing.**
   Every event is printed with a millisecond timestamp, the thread tag, the action, and the live
   load of the store.

---

## 3. Flow at a glance

```

[Writer-1  Writer-2  Writer-3  Writer-4]
        |
        v   put()  -> acquire(_vacant)
+--------------------------------------------------+
|            MUTEX  (_gate)                         |
|     deque store   [ capacity = 8 payloads ]       |
+--------------------------------------------------+
        ^   take()  -> acquire(_ready)
        |
[Reader-1  Reader-2  Reader-3]

```

---

## 4. Running it
Needs Python 3.8 or newer:

```bash
cd "Chapter 2"
python bounded_buffer_monitor.py
```

### Sample trace (timings vary between runs)

```text
------------------------------------------------------------------------------
  PDC ASSIGNMENT 2  :  Bounded-Store Writer / Reader Coordination
  Store capacity = 8   |   Total payloads = 24
------------------------------------------------------------------------------
19:51:12.750  | Reader-1  |  ONLINE          ->  worker thread started
19:51:12.751  | Reader-2  |  ONLINE          ->  worker thread started
19:51:12.753  | Reader-3  |  ONLINE          ->  worker thread started
19:51:12.754  | Writer-1  |  ONLINE          ->  worker thread started
19:51:12.858  | Writer-2  |  ENQUEUED        ->  W2:item-1  (load 1/8)
19:51:12.858  | Reader-1  |  DEQUEUED        ->  W2:item-1  (load 0/8)
19:51:13.016  | Writer-4  |  ENQUEUED        ->  W4:item-1  (load 1/8)
19:51:13.016  | Reader-2  |  DEQUEUED        ->  W4:item-1  (load 0/8)
...
19:51:16.912  | Reader-2  |  RETIRED         ->  drained and exited
19:51:16.930  | Reader-3  |  RETIRED         ->  drained and exited
------------------------------------------------------------------------------
  Wall-clock time : 4.19 s
  Leftover in store : 0 payload(s)
------------------------------------------------------------------------------
```
