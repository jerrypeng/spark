---
layout: global
title: Streaming Shuffle
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* This will become a table of contents (this text will be scrubbed).
{:toc}

# Overview

Streaming shuffle is an alternative `ShuffleManager` for low-latency,
continuously-running queries (for example, real-time mode in Structured
Streaming). Unlike the default sort-based shuffle, it does not materialize map
outputs to disk and does not require map tasks to finish before reduce tasks
can start. Each map task hosts a network server that pushes records to reduce
tasks as they are produced; reduce tasks open clients to those servers and
consume records as a stream.

# When To Use It

Use streaming shuffle when **all** of the following hold:

* The job is long-running and can keep all map and reduce tasks resident at
  the same time. Streaming shuffle requires concurrent stage execution; map
  and reduce stages do not run sequentially as they do under sort shuffle.
* End-to-end latency matters more than peak throughput per task. Sort shuffle
  is almost always faster for batch jobs that can afford to checkpoint to disk.
* Recovery semantics are managed at a higher level (for example, by the
  micro-batch boundaries of Structured Streaming). Streaming shuffle has no
  on-disk state, so a single task failure forces an upstream replay.

For all other workloads, prefer the default `SortShuffleManager`. If a single
cluster needs to mix streaming and batch jobs, the
[`MultiShuffleManager`](#mixing-batch-and-streaming-shuffle) routes per-query.

# Architecture

A streaming shuffle is established by three actors:

* The **driver** runs `StreamingShuffleOutputTrackerMaster`, which holds the
  set of registered shuffles and the network locations of every writer task.
* Each **map task** runs a `StreamingShuffleWriter`. The writer starts a
  `TransportServer` on an OS-assigned port, registers its
  `(executorId, host, port)` with the tracker, and pushes records as
  `DataMessage`s to whichever readers are connected.
* Each **reduce task** runs a `StreamingShuffleReader`. The reader polls the
  tracker for writer locations, opens a `TransportClient` to each writer in
  parallel, and consumes `DataMessage`s into an in-memory queue that the
  task's output iterator drains.

```
                ┌──────────────────────────────────────────────┐
                │                Driver                        │
                │  StreamingShuffleOutputTrackerMaster (RPC)   │
                └──────────┬─────────────────────────┬─────────┘
            register(...)  │                         │  lookup
                           ▼                         ▼
       ┌──────────────────────────┐   ┌──────────────────────────┐
       │  Map task 0              │   │  Reduce task 0           │
       │  StreamingShuffleWriter  │◀──│  StreamingShuffleReader  │
       │  TransportServer:p0      │   │  TransportClient*        │
       └──────────────────────────┘   └──────────────────────────┘
                  ▲                                ▲
                  │ DataMessage                    │ DataMessage
                  │ TerminationControl             │ from each writer
                  │ TerminationAck (received)      │
                  ▼                                ▼
       ┌──────────────────────────┐   ┌──────────────────────────┐
       │  Map task 1              │   │  Reduce task 1           │
       │  StreamingShuffleWriter  │◀──│  StreamingShuffleReader  │
       │  TransportServer:p1      │   │  ...                     │
       └──────────────────────────┘   └──────────────────────────┘
```

The map and reduce stages **must run concurrently**. The `DAGScheduler`
registers the shuffle with the tracker when it creates the shuffle map stage,
but does not gate the reduce stage on map completion the way it does for sort
shuffle.

# Wire Protocol

The wire format is defined in package
`org.apache.spark.network.shuffle.streaming`. All messages share a 12-byte
common header followed by a message-type-specific payload:

| Bytes | Field         | Description                                                          |
|-------|---------------|----------------------------------------------------------------------|
| 0–3   | message type  | `int`; one of the values in `StreamingShuffleMessageType`.           |
| 4–11  | sequence num. | `long`; assigned by the sender, monotonic per (writer, reader) pair. |

The four concrete message types:

| Type                          | Direction       | Purpose                                                            |
|-------------------------------|-----------------|--------------------------------------------------------------------|
| `DATA_MESSAGE_UNSAFE_ROW`     | writer → reader | A buffer of serialized records.                                    |
| `CREDIT_CONTROL_MESSAGE`      | reader → writer | "I am ready"; sent on connection establishment.                    |
| `TERMINATION_CONTROL_MESSAGE` | writer → reader | "No more data is coming."                                          |
| `TERMINATION_ACK_MESSAGE`     | reader → writer | Echoes the last sequence number the reader observed.               |

## DataMessage layout

```
+---+---+---+---+---+---+---+---+---+---+---+---+
| messageType   | sequenceNumber                |  (12 B common header)
+---+---+---+---+---+---+---+---+---+---+---+---+
| shuffleWriterId               |
+---+---+---+---+
| shuffleReaderId               |
+---+---+---+---+
| dataSize                      |
+---+---+---+---+
| checksum (CRC32C, long)                       |
+---+---+---+---+---+---+---+---+
| <dataSize> bytes of serialized records ...
+----------------------------------------------------------
```

Records are serialized with the dependency's `Serializer` into a single
contiguous buffer. The reader deserializes them in order.

## Sequence numbers

Every message a writer sends to a given reader carries a sequence number
starting at 0 and incrementing by 1 per message. The reader's
`StreamingShuffleClientHandler` enforces strict consecutive ordering; any
gap, duplicate, or reorder fails the task with the
[`STREAMING_SHUFFLE_INCORRECT_SEQUENCE_NUMBER`](sql-error-conditions.html)
error.

# Coordination

`StreamingShuffleOutputTracker` is the driver-side service that introduces
writers and readers to each other. It is a per-`SparkContext` singleton,
constructed during `SparkEnv` initialization when the configured
`spark.shuffle.manager` is `StreamingShuffleManager` or `MultiShuffleManager`.

| API                                                      | Caller               | Behavior                                                |
|----------------------------------------------------------|----------------------|---------------------------------------------------------|
| `registerShuffle(shuffleId, numMaps, numReduces, jobId)` | DAGScheduler         | Records shuffle metadata.                               |
| `registerShuffleWriterTask(shuffleId, mapId, location)`  | Writer task          | Publishes the writer's `(executorId, host, port)`.      |
| `getAllShuffleWriterTaskLocations(shuffleId)`            | Reader (barrier)     | Returns `None` until **all** writers have registered.   |
| `getAvailableShuffleWriterTaskLocations(shuffleId)`      | Reader (progressive) | Returns whatever writers have registered so far.        |

The reader uses the **progressive** API so it can begin consuming from the
first writer while later writers are still launching. As soon as a new writer
appears in the tracker, the reader's background discovery thread opens a
client to it and starts consuming.

# Backpressure And Flow Control

Backpressure flows from the slowest reader back to the source iterator on the
writer side via two cooperating mechanisms:

* **Reader-side TCP backpressure.** The reader maintains a per-writer byte
  budget of `spark.shuffle.streaming.readerMaxMemory / numWriters`. Receiving a
  `DataMessage` shrinks the budget; releasing the buffer after consumption
  grows it back. Crossing zero flips the channel's `autoRead` to `false`,
  which stops the kernel from acknowledging further bytes and ultimately
  shrinks the writer's send window.
* **Writer-side memory bound.** A counting semaphore caps total in-flight
  bytes. Its capacity is `spark.shuffle.streaming.writerMaxMemory` minus a
  fixed allowance for TCP send/receive buffers, with a floor of two
  `networkBufferSize`-sized buffers per partition. When all permits are used,
  the next buffer allocation blocks the write loop until a previously-sent
  buffer is acknowledged by Netty's listener and returned to the pool.

The two compose: a slow reader fills its socket buffers, which prevents the
writer's `OneWayMessage` from completing, which holds permits in the writer's
semaphore, which blocks the upstream iterator.

`CreditControlMessage` is sent by the reader on connection establishment as
the initial handshake; it carries no numeric credit value today but is
reserved as a hook for a future windowed flow-control scheme.

# Termination Handshake

When a writer's iterator is exhausted it sends one
`TerminationControlMessage` per reader and waits for a matching
`TerminationAckMessage` back. The ack carries the last sequence number the
reader observed; the writer compares it against its own last-sent sequence
number and fails the task with
[`STREAMING_SHUFFLE_INCORRECT_SEQUENCE_NUMBER`](sql-error-conditions.html)
on mismatch. A reader exits its read loop once it has received a
`TerminationControlMessage` from every writer registered with the tracker.

# Memory Management

Both writer and reader register a `MemoryConsumer` against the task's
`TaskMemoryManager`, allocating off-heap (`MemoryMode.OFF_HEAP`). The
`spill()` callback returns 0 because streaming shuffle never spills: a slow
reader is handled by backpressure, not by writing to disk.

On the writer side, each reducer partition has a dedicated `TimestampedBuffer`
filled with serialized rows. The buffer is sent as a `DataMessage` when
either:

1. Its size reaches 90% of `spark.shuffle.streaming.networkBufferSize`, or
2. Its age reaches
   `spark.shuffle.streaming.networkBufferMaxWaitTimeMs` — the time-based
   flush thread checks every interval and flushes any stale buffer.

After send, the underlying `ByteBuf` is returned to a per-writer pool unless
the task has failed or completed.

# Checksum Integrity

When `spark.shuffle.streaming.checksum.enabled` is `true` (the default) the
writer maintains a CRC32C over the serialized payload and stamps it into the
`DataMessage` header. The reader recomputes the checksum after deserialization
and fails the task with
[`STREAMING_SHUFFLE_CHECKSUM_VERIFICATION_FAILED`](sql-error-conditions.html)
on mismatch. Disabling the check trades integrity for a measurable
per-message CPU saving; do this only on networks you trust to be lossless.

# Mixing Batch And Streaming Shuffle

`MultiShuffleManager` is a routing wrapper around both `SortShuffleManager`
and `StreamingShuffleManager`. It selects the underlying manager per shuffle
based on a per-job task-local property:

```
spark.shuffle.streaming.useForCurrentQuery = true   # streaming shuffle
spark.shuffle.streaming.useForCurrentQuery = false  # sort shuffle (default)
```

The routing decision is cached per `shuffleId`, so every
`registerShuffle` / `getWriter` / `getReader` call for the same shuffle
resolves to the same underlying manager. This is the recommended setting on
clusters that need to run both batch and streaming workloads without a
restart.

To enable:

```
--conf spark.shuffle.manager=org.apache.spark.shuffle.streaming.MultiShuffleManager
```

Set `spark.shuffle.streaming.useForCurrentQuery=true` on the SparkContext (or
as a TaskContext local property on executors) for queries that require
streaming shuffle.

# Configuration Reference

<table class="spark-config">
<tr><th style="width:24%">Property Name</th><th style="width:14%">Default</th><th>Meaning</th><th>Since Version</th></tr>
<tr>
  <td><code>spark.shuffle.streaming.checksum.enabled</code></td>
  <td><code>true</code></td>
  <td>
    Append a CRC32C checksum to each streaming shuffle data buffer. The
    writer computes the checksum and embeds it in the <code>DataMessage</code>
    header; the reader recomputes and compares. A mismatch fails the task,
    providing early detection of data corruption in transit.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.networkBufferSize</code></td>
  <td><code>32768</code> (32 KB)</td>
  <td>
    Target byte size for each network buffer sent from a streaming shuffle
    writer to a reader. Larger values reduce per-message overhead; smaller
    values reduce latency.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.networkBufferMaxWaitTimeMs</code></td>
  <td><code>50</code></td>
  <td>
    Maximum time in milliseconds a partially-filled network buffer is held
    before being flushed to the reader. Lower values reduce latency at the
    cost of smaller, less efficient messages.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.writerMaxMemory</code></td>
  <td><code>33554432</code> (32 MB)</td>
  <td>
    Best-effort memory limit in bytes for in-flight data buffers in a
    streaming shuffle writer task. Includes TCP send/receive buffers. The
    writer back-pressures the upstream iterator when this limit is reached.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.readerMaxMemory</code></td>
  <td><code>33554432</code> (32 MB)</td>
  <td>
    Best-effort memory limit in bytes for data buffered in a streaming
    shuffle reader task. The per-writer byte quota is derived from this value
    divided by the number of shuffle writers. When the quota is exhausted the
    reader applies TCP back-pressure.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.shuffle.streaming.useForCurrentQuery</code></td>
  <td><code>false</code></td>
  <td>
    Used only with <code>MultiShuffleManager</code>. When set to
    <code>true</code> on the SparkContext (driver) or as a task-local
    property (executors), the next shuffle to be registered is routed to
    <code>StreamingShuffleManager</code>. The routing decision is cached
    per-shuffle.
  </td>
  <td>4.0.0</td>
</tr>
</table>

# Comparison With Sort Shuffle

| Aspect                   | Sort shuffle                              | Streaming shuffle                                         |
|--------------------------|-------------------------------------------|-----------------------------------------------------------|
| Map output material.     | Local files, indexed by reducer           | None; pushed straight to readers                          |
| Stage execution          | Map stage finishes before reduce starts   | Map and reduce stages run concurrently                    |
| Recovery on task failure | Re-run failed task; outputs survive       | Replay the upstream source                                |
| Backpressure             | None on the write path                    | Reader → writer via TCP `autoRead` and a memory semaphore |
| Disk usage               | Up to the full intermediate dataset       | Effectively zero                                          |
| Latency floor            | Bound by map-stage completion             | Bound by per-buffer flush interval (ms)                   |
| Suited for               | Batch jobs                                | Real-time and continuous queries                          |
