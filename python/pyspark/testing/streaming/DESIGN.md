# PySpark Native StreamTest Framework -- Design Doc

A Python port of Scala's `StreamTest` trait
(`sql/core/src/test/scala/org/apache/spark/sql/streaming/StreamTest.scala`)
for testing PySpark Structured Streaming queries. Delivered as a 4-PR
stack against `apache/spark` master.

## 1. Goal

Scala's `StreamTest` provides a declarative, action-based testing
paradigm used by 38+ test suites. PySpark previously had no equivalent:
streaming tests were ad-hoc `writeStream` + `processAllAvailable` +
`time.sleep` loops. This framework brings the same paradigm to Python
with an idiomatic API (`unittest.TestCase` base, dataclass-style action
objects, full type hints).

The framework is **JVM-only**. Spark Connect is out of scope to match
Scala's `StreamTest`, which is also JVM-only.

## 2. Architecture

```
+--------------------------------------------------------------+
|              Test author (Python unittest)                   |
|                                                              |
|   class MyTest(StreamTest):                                  |
|       def test_double(self):                                 |
|           source = MemoryStream(self.spark, "int")           |
|           df = source.to_df().selectExpr("value*2 as value") |
|           self.run_stream_test(                              |
|               df,                                            |
|               AddData(source, 1, 2, 3),                      |
|               CheckAnswer(2, 4, 6),                          |
|           )                                                  |
+----------------------------+---------------------------------+
                             v
+--------------------------------------------------------------+
|       pyspark.testing.streaming  (Python framework)          |
|                                                              |
|   StreamTest -> _Runner._execute_action loop                 |
|       lifecycle: StartStream/StopStream/AddData/             |
|                  ProcessAllAvailable                         |
|       assertions: Assert/AssertOnQuery/Execute/ExpectFailure |
|       checks: CheckAnswer/CheckLastBatch/CheckNewAnswer/     |
|               CheckAnswerByFunc                              |
|       RTM: CheckAnswer{With,Contains}Timeout/                |
|            ExternalAction/Sleep                              |
|                                                              |
|   MemoryStream / LowLatencyMemoryStream (Python wrappers)    |
|   ContinuousMemorySink (Python wrapper)                      |
+----------------------------+---------------------------------+
                             |  Py4J
                             v
+--------------------------------------------------------------+
|     PythonSQLUtils  (Scala bridge, +14 test-only helpers)    |
+----------------------------+---------------------------------+
                             v
+--------------------------------------------------------------+
|   JVM streaming runtime: MemoryStream, MemorySink,           |
|   LowLatencyMemoryStream, ContinuousMemorySink, triggers,    |
|   StreamingQueryManager.startQuery (all unchanged)           |
+--------------------------------------------------------------+
```

### Driver loop (`_Runner` in `stream_test.py`)

`StreamTest.run_stream_test(stream, *actions, output_mode, extra_options, sink)`
constructs a `_Runner` and walks the action list synchronously. Key
behaviors:

1. **Auto-inserts `StartStream`** before the first action that needs a
   running stream.
2. **Per-test isolation**: unique query name (`stream_test_{pid}_{ns}`),
   temp checkpoint dir, sink-table drop in `tearDown`.
3. **Two routing paths**:
   - Standard: `writeStream.format("memory")`; sink read via
     `_MemorySinkReader` -> JVM `memorySink*` helpers.
   - Custom-sink (RTM): `startStreamingQueryWithSink` attaches a
     user-supplied `ContinuousMemorySink`; sink read directly.
4. **Cumulative cursor (`_last_fetched_rows`)** drives `CheckNewAnswer`
   and is preserved across `StopStream` + `StartStream` so MemoryStream
   replay rows aren't double-reported. Reset on restart in
   complete/update modes (the sink truncates per batch).
5. **Per-call `latestBatchId`** drives `CheckLastBatch`, scoped to the
   current run.
6. **Checkpoint wipe on every `StartStream`**: `MemoryStream` has no
   checkpoint recovery (offsets vanish on restart).
7. **State dump on failure** with `=>` cursor on the failing action,
   query state, and captured exception. Falls back to `last_query` so
   failures after `StopStream` / `ExpectFailure` still surface context.

### Why JVM bridges instead of pure Python?

`MemoryStream` and `MemorySink` are battle-tested JVM classes used by
thousands of Scala tests. Reimplementing offset tracking, batch
slicing, and commit semantics in Python via `SimpleDataSourceStreamReader`
would be a multi-month effort with unclear correctness. A thin Py4J
bridge gets full fidelity for free.

The sink also dictates the bridge approach: an early version read via
`SELECT * FROM <queryName>`, but `LocalTableScanExec` does not preserve
batch arrival order, so per-batch row slicing was unsound. The fix
goes directly to `MemorySink`'s `synchronized` accessors.

## 3. PR Stack

| Branch | Purpose | Files | Tests |
|---|---|---|---|
| `stack/streamtest-py-1-bridge` | JVM bridge + `MemoryStream` Python wrapper | 6 | 11 |
| `stack/streamtest-py-2-base` | `StreamTest` base + lifecycle/assertion actions | +4 | +12 |
| `stack/streamtest-py-3-checks` | `CheckAnswer` family + flexible-type expected values | +5 | +26 |
| `stack/streamtest-py-4-rtm` | Real-Time Mode (LowLatencyMemoryStream, ContinuousMemorySink, polling) | +6 | +13 |

**Total**: 3,311 insertions across 9 production files; **62 tests** at the
tip of PR 4.

### Action catalog

| PR | Action | Behavior |
|---|---|---|
| 2 | `StartStream(trigger, extra_options, checkpoint_location)` | Start / restart |
| 2 | `StopStream` | Stop with clean-shutdown assertion |
| 2 | `AddData(source, *rows)` | Append to a `MemoryStream` (PR 4 also accepts `LowLatencyMemoryStream`) |
| 2 | `ProcessAllAvailable` | Block on `query.processAllAvailable()` |
| 2 | `ExpectFailure(exception_type, assert_failure)` | Wait for terminate-with-error |
| 2 | `Assert(condition, message)` | Zero-arg predicate |
| 2 | `AssertOnQuery(condition, message)` | Predicate over current/last query |
| 2 | `Execute(func, name)` | Callback receiving the query |
| 3 | `CheckAnswer(*data, ordered=False)` | Multiset-equal cumulative sink |
| 3 | `CheckLastBatch(*data, ordered=False)` | Multiset-equal latest batch only |
| 3 | `CheckNewAnswer(*data, ordered=False)` | Delta since previous check |
| 3 | `CheckAnswerByFunc(check_func, last_only=False)` | Custom `(List[Row]) -> None` predicate |
| 4 | `CheckAnswerWithTimeout(timeout_ms, *data)` | Poll until match or timeout |
| 4 | `CheckAnswerContainsWithTimeout(timeout_ms, *data)` | Poll until expected is a multiset subset |
| 4 | `ExternalAction(func, name)` / `Sleep(seconds)` | Side-effecting callbacks for RTM tests |

### Flexible expected-row shapes (PR 3, mirrors Scala's `CheckAnswer[A: Encoder]`)

`CheckAnswer` and friends accept: scalar (single-column), `Row`, tuple,
dict, dataclass, namedtuple (resolved by **field name**, not
positionally), `typing.NamedTuple`, plain class with attributes
matching the schema, and any mix.

### RTM (PR 4)

`format("memory")` always allocates a per-batch-buffering `MemorySink`
and the standard `MemoryStream` does not implement `SupportsRealTimeMode`,
so RTM needs three distinct Python-side pieces: a `LowLatencyMemoryStream`
source, a `ContinuousMemorySink` (continuous, exposes data as written),
and a `run_stream_test(sink=...)` parameter that routes through
`startStreamingQueryWithSink` instead of `writeStream`.

`_check_with_timeout`'s polling loop fails fast on a dead query
(distinguishing "terminated with exception" vs "terminated cleanly
before producing rows" vs "still timing out"), and a final liveness
check after the deadline catches a crash that lands in the gap between
the last in-loop check and the deadline expiry.

`CheckLastBatch` and `CheckNewAnswer` are explicitly rejected on the
custom-sink path (`ContinuousMemorySink` doesn't expose batch ids); the
error message points at the timeout-polling pair.

Trigger validation (shared between standard and RTM paths via
`_validate_trigger_keys`): rejects unknown keys, multiple
mutually-exclusive keys, and falsy `once` / `availableNow` values.

## 4. Test coverage

| Class | Tests | Focus |
|---|---|---|
| `MemoryStreamTests` | 10 | Schema variants, offset tracking, namedtuple-by-name regression |
| `StreamTestLifecycleTests` | 12 | Auto-start, restart, `Assert`/`AssertOnQuery`/`Execute`/`ExpectFailure`, state dump, batch-DataFrame rejection |
| `CheckActionTests` | 15 | Map/filter/aggregations, all Check actions, restart, cursor ordering, empty-batch error |
| `CheckAnswerFlexibleTypesTests` | 10 | All input shapes (scalar/tuple/dict/dataclass/namedtuple/...) |
| `CheckActionErrorReportingTests` | 1 | Row-mismatch diff format |
| `RtmTests` | 13 | LowLatencyMemoryStream + ContinuousMemorySink + Trigger.RealTime, polling Check actions, cross-sink rejections, sink reuse-clear |
| `ReusedSQLTestCase` (inherited) | 1 | Base-class probe |

**Total: 62 tests**, ~25 s end-to-end via
`python3.10 -m unittest pyspark.sql.tests.streaming.test_stream_test_framework`.

## 5. Usage

### Basic

```python
from pyspark.testing.streaming import (
    StreamTest, MemoryStream, AddData, CheckAnswer,
)

class WordCountTest(StreamTest):
    def test_count(self):
        source = MemoryStream(self.spark, "string")
        counts = source.to_df().groupBy("value").count()
        self.run_stream_test(
            counts,
            AddData(source, "a", "b", "a"),
            CheckAnswer(("a", 2), ("b", 1)),
            output_mode="complete",
        )
```

### Flexible expected-row shapes

`CheckAnswer` accepts whatever shape is most natural at the call site --
scalars (single-column schemas), tuples, dicts, dataclasses, namedtuples
(matched by field name, not position), `Row`, or any mix:

```python
from collections import namedtuple
from dataclasses import dataclass
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing.streaming import StreamTest, MemoryStream, AddData, CheckAnswer

PERSON = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
])

@dataclass
class Person:
    name: str
    age: int

PersonNT = namedtuple("PersonNT", ["age", "name"])  # fields reversed!

class FlexibleShapesTest(StreamTest):
    def test_scalars(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df().selectExpr("value + 1 as value"),
            AddData(source, 1, 2, 3),
            CheckAnswer(2, 3, 4),  # scalars for a single-column schema
        )

    def test_tuples_and_objects(self):
        source = MemoryStream(self.spark, PERSON)
        self.run_stream_test(
            source.to_df(),
            AddData(source, ("Alice", 30), {"name": "Bob", "age": 25}),
            CheckAnswer(Person("Alice", 30), PersonNT(age=25, name="Bob")),
        )
```

### Stop, restart, and per-batch checks

`StopStream` + `StartStream` resumes against the same source state.
`CheckLastBatch` inspects only rows from the most recent committed batch;
`CheckNewAnswer` returns rows added since the previous Check action:

```python
from pyspark.testing.streaming import (
    StreamTest, MemoryStream, AddData, CheckAnswer, CheckLastBatch,
    CheckNewAnswer, StopStream, StartStream,
)

class RestartTest(StreamTest):
    def test_stop_restart(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2),
            CheckLastBatch(1, 2),       # rows from this batch only
            AddData(source, 3),
            CheckNewAnswer(3),          # delta since previous Check
            StopStream(),
            AddData(source, 4),
            StartStream(),              # checkpoint wiped; replays 1..4
            CheckAnswer(1, 2, 3, 4),    # cumulative
        )
```

### Inspecting query state

`AssertOnQuery` and `Execute` give access to the live `StreamingQuery`:

```python
from pyspark.testing.streaming import (
    StreamTest, MemoryStream, AddData, AssertOnQuery, Execute,
    ProcessAllAvailable,
)

class InspectionTest(StreamTest):
    def test_capture_progress(self):
        captured = []
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2, 3),
            ProcessAllAvailable(),
            AssertOnQuery(lambda q: q.lastProgress is not None,
                          "expected progress event"),
            Execute(
                lambda q: captured.append(q.lastProgress["numInputRows"]),
                name="capture-numInputRows",
            ),
        )
        assert captured == [3]
```

### Custom validation with `CheckAnswerByFunc`

When the assertion logic is more nuanced than a multiset compare,
`CheckAnswerByFunc` accepts an arbitrary `(List[Row]) -> None`
predicate that raises on failure:

```python
from pyspark.testing.streaming import (
    StreamTest, MemoryStream, AddData, CheckAnswerByFunc,
)

class CustomCheckTest(StreamTest):
    def test_average(self):
        source = MemoryStream(self.spark, "int")

        def check(rows):
            avg = sum(r.value for r in rows) / len(rows)
            assert 4.5 <= avg <= 5.5, f"avg out of range: {avg}"

        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 5, 9),
            CheckAnswerByFunc(check),
        )
```

### Failure expectations

```python
from pyspark.errors import StreamingQueryException
from pyspark.testing.streaming import (
    StreamTest, MemoryStream, AddData, ExpectFailure,
)

class FailureTest(StreamTest):
    def test_raise_error(self):
        source = MemoryStream(self.spark, "int")
        df = source.to_df().selectExpr("raise_error('boom') as value")
        self.run_stream_test(
            df,
            AddData(source, 1),
            ExpectFailure(StreamingQueryException,
                          assert_failure=lambda e: "boom" in str(e)),
        )
```

### Real-Time Mode

```python
from pyspark.testing.streaming import (
    StreamTest, LowLatencyMemoryStream, ContinuousMemorySink,
    StartStream, AddData, CheckAnswerWithTimeout,
)

class RtmTest(StreamTest):
    spark_conf = {"spark.sql.streaming.realTimeMode.minBatchDuration": "100"}

    def test_rtm(self):
        source = LowLatencyMemoryStream(self.spark, "int", num_partitions=2)
        sink = ContinuousMemorySink(self.spark)
        self.run_stream_test(
            source.to_df(),
            StartStream(trigger={"realTime": "200 milliseconds"}),
            AddData(source, 1, 2, 3),
            CheckAnswerWithTimeout(10_000, 1, 2, 3),
            output_mode="update",
            sink=sink,
        )
```

## 6. Build and test

```bash
# Java 17, SBT 1.12.4 (pinned in project/build.properties)
build/sbt -Phive package

PYTHONPATH="python:python/lib/py4j-0.10.9.9-src.zip" \
  SPARK_HOME=. PYSPARK_PYTHON=python3.10 \
  python3.10 -m unittest pyspark.sql.tests.streaming.test_stream_test_framework
```

Per-PR sanity check (each branch must compile and pass its own tests):

```bash
for B in stack/streamtest-py-{1-bridge,2-base,3-checks,4-rtm}; do
    git checkout "$B" && build/sbt -Phive package && \
    PYTHONPATH="python:python/lib/py4j-0.10.9.9-src.zip" \
      SPARK_HOME=. PYSPARK_PYTHON=python3.10 \
      python3.10 -m unittest pyspark.sql.tests.streaming.test_stream_test_framework
done
# Expected: 11, 23, 49, 62 tests pass.
```
