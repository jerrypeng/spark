#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Self-tests for the PySpark ``StreamTest`` framework.

Covers the ``MemoryStream`` Python wrapper (driven directly against the
JVM bridge with vanilla ``writeStream.format("memory")``) and the
``StreamTest`` driver: lifecycle and assertion actions, the
``CheckAnswer`` family with flexible expected-row shapes, and Real-Time
Mode (``LowLatencyMemoryStream`` + ``ContinuousMemorySink`` +
``Trigger.RealTime``). Each commit in the framework's PR stack adds the
test classes for the API it introduces.
"""

import os
import shutil
import tempfile
import time
from collections import namedtuple
from dataclasses import dataclass
from typing import Any, List, NamedTuple

from pyspark.errors import StreamingQueryException
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.streaming import (
    AddData,
    Assert,
    AssertOnQuery,
    CheckAnswer,
    CheckAnswerByFunc,
    CheckAnswerContainsWithTimeout,
    CheckAnswerWithTimeout,
    CheckLastBatch,
    CheckNewAnswer,
    ContinuousMemorySink,
    Execute,
    ExpectFailure,
    ExternalAction,
    LowLatencyMemoryStream,
    MemoryStream,
    ProcessAllAvailable,
    Sleep,
    StartStream,
    StopStream,
    StreamTest,
)


class MemoryStreamTests(ReusedSQLTestCase):
    """Self-tests for the ``MemoryStream`` Python wrapper."""

    def setUp(self):
        super().setUp()
        self._checkpoint = tempfile.mkdtemp(prefix="memory_stream_test_")
        self._query_name = f"memory_stream_test_{os.getpid()}_{time.monotonic_ns()}"

    def tearDown(self):
        try:
            for q in self.spark.streams.active:
                if q.name == self._query_name:
                    q.stop()
        finally:
            shutil.rmtree(self._checkpoint, ignore_errors=True)
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {self._query_name}")
            except Exception:
                pass
        super().tearDown()

    def _start_passthrough(self, df):
        return (
            df.writeStream.format("memory")
            .queryName(self._query_name)
            .option("checkpointLocation", self._checkpoint)
            .trigger(processingTime="0 seconds")
            .outputMode("append")
            .start()
        )

    def test_simple_int_schema_round_trip(self):
        source = MemoryStream(self.spark, "int")
        self.assertEqual(source.schema.simpleString(), "struct<value:int>")
        offset = source.add_data(1, 2, 3)
        self.assertEqual(offset, 0)

        query = self._start_passthrough(source.to_df())
        try:
            query.processAllAvailable()
            rows = self.spark.sql(f"SELECT value FROM {self._query_name}").collect()
            self.assertEqual(sorted(r.value for r in rows), [1, 2, 3])
        finally:
            query.stop()
            query.awaitTermination(60)

    def test_subsequent_add_data_advances_offset(self):
        source = MemoryStream(self.spark, "int")
        self.assertEqual(source.add_data(1), 0)
        self.assertEqual(source.add_data(2, 3), 1)
        self.assertEqual(source.add_data(4, 5, 6), 2)
        self.assertEqual(source.current_offset, 2)

    def test_empty_add_data_is_noop(self):
        source = MemoryStream(self.spark, "int")
        self.assertEqual(source.add_data(), -1)
        # First real call still gets offset 0.
        self.assertEqual(source.add_data(1), 0)

    def test_struct_schema(self):
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        source = MemoryStream(self.spark, schema)
        source.add_data(Row(name="Alice", age=30), Row(name="Bob", age=25))

        query = self._start_passthrough(source.to_df())
        try:
            query.processAllAvailable()
            rows = self.spark.sql(
                f"SELECT name, age FROM {self._query_name} ORDER BY name"
            ).collect()
            self.assertEqual([(r.name, r.age) for r in rows], [("Alice", 30), ("Bob", 25)])
        finally:
            query.stop()
            query.awaitTermination(60)

    def test_struct_schema_accepts_tuples_and_dicts(self):
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        source = MemoryStream(self.spark, schema)
        # Tuples positional, dicts by name.
        source.add_data(("Alice", 30), {"name": "Bob", "age": 25})
        query = self._start_passthrough(source.to_df())
        try:
            query.processAllAvailable()
            rows = self.spark.sql(
                f"SELECT name, age FROM {self._query_name} ORDER BY name"
            ).collect()
            self.assertEqual([(r.name, r.age) for r in rows], [("Alice", 30), ("Bob", 25)])
        finally:
            query.stop()
            query.awaitTermination(60)

    def test_unsupported_simple_type_rejected(self):
        with self.assertRaises(ValueError):
            MemoryStream(self.spark, "uuid")

    def test_unsupported_schema_arg_type_rejected(self):
        with self.assertRaises(TypeError):
            MemoryStream(self.spark, schema=123)  # type: ignore[arg-type]

    def test_to_df_is_streaming(self):
        source = MemoryStream(self.spark, "int")
        df = source.to_df()
        self.assertTrue(df.isStreaming)
        self.assertEqual(df.schema.simpleString(), "struct<value:int>")

    def test_namedtuple_resolved_by_field_name_not_position(self):
        """Regression: namedtuples must be matched against the schema by
        field name, not positionally. A namedtuple whose fields are in a
        different order than the schema would otherwise silently produce
        mis-mapped Rows."""
        # PT's field order is the *opposite* of the schema's. Positional
        # mapping would put the int "30" into ``name`` and the string
        # "Alice" into ``age``, producing a type-coercion failure or
        # garbled output. Resolving by name yields the correct Row.
        PT = namedtuple("PT", ["age", "name"])
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        source = MemoryStream(self.spark, schema)
        source.add_data(PT(age=30, name="Alice"))

        query = self._start_passthrough(source.to_df())
        try:
            query.processAllAvailable()
            rows = self.spark.sql(f"SELECT name, age FROM {self._query_name}").collect()
            self.assertEqual([(r.name, r.age) for r in rows], [("Alice", 30)])
        finally:
            query.stop()
            query.awaitTermination(60)


# ---------------------------------------------------------------------------
# StreamTest driver -- lifecycle & assertion actions
# ---------------------------------------------------------------------------


class StreamTestLifecycleTests(StreamTest):
    """Tests for ``StreamTest``'s driver and lifecycle/assertion actions."""

    def test_explicit_start_stream(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            StartStream(),
            AddData(source, 1, 2),
            ProcessAllAvailable(),
        )

    def test_auto_start_inserts_start_stream(self):
        source = MemoryStream(self.spark, "int")
        # No explicit StartStream -- the driver should insert one before
        # the first action that requires a running stream.
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2),
            ProcessAllAvailable(),
        )

    def test_stop_and_restart(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2),
            ProcessAllAvailable(),
            StopStream(),
            AddData(source, 3),
            StartStream(),
            ProcessAllAvailable(),
        )

    def test_assert_on_query_active(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1),
            AssertOnQuery(lambda q: q.isActive, "query should be active"),
            ProcessAllAvailable(),
            AssertOnQuery(
                lambda q: q.lastProgress is not None,
                "lastProgress should be populated after processing",
            ),
        )

    def test_assert_on_query_after_stop(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1),
            ProcessAllAvailable(),
            StopStream(),
            # AssertOnQuery sees the just-stopped query.
            AssertOnQuery(lambda q: not q.isActive, "query should be stopped"),
        )

    def test_assert_callback(self):
        flag: List[bool] = []

        def set_flag() -> bool:
            flag.append(True)
            return True

        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1),
            Assert(set_flag, "side-effect should run"),
            Assert(lambda: bool(flag), "side-effect should still be present"),
        )
        self.assertTrue(flag, "Assert callback should have executed")

    def test_execute(self):
        captured: List[Any] = []
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1),
            ProcessAllAvailable(),
            Execute(lambda q: captured.append(q.lastProgress), name="capture"),
        )
        self.assertEqual(len(captured), 1)
        self.assertIsNotNone(captured[0])

    def test_expect_failure(self):
        source = MemoryStream(self.spark, "int")
        # raise_error() deterministically fails the query regardless of the
        # session's ANSI mode setting. The query thread surfaces a
        # StreamingQueryException, which is what query.exception() returns.
        df = source.to_df().selectExpr("raise_error('boom') as value")
        captured: List[str] = []
        self.run_stream_test(
            df,
            AddData(source, 1, 2, 3),
            ExpectFailure(
                StreamingQueryException,
                assert_failure=lambda e: captured.append(str(e)),
            ),
        )
        self.assertTrue(captured, "assert_failure callback should have run")
        self.assertIn("boom", captured[0])

    def test_assert_failure_includes_state_dump(self):
        source = MemoryStream(self.spark, "int")
        with self.assertRaises(AssertionError) as ctx:
            self.run_stream_test(
                source.to_df(),
                AddData(source, 1),
                Assert(lambda: False, "always fails"),
            )
        msg = str(ctx.exception)
        self.assertIn("Assert failed: always fails", msg)
        # The progress trace should highlight the failing action.
        self.assertIn("=> Assert", msg)

    def test_batch_dataframe_rejected(self):
        df = self.spark.range(3).toDF("value")
        with self.assertRaises(ValueError):
            self.run_stream_test(df, ProcessAllAvailable())

    def test_assert_propagates_arbitrary_exception(self):
        """A non-AssertionError raised inside Assert.condition must surface
        with the original cause folded into the AssertionError message."""
        source = MemoryStream(self.spark, "int")

        def boom() -> bool:
            raise RuntimeError("kaboom")

        with self.assertRaises(AssertionError) as ctx:
            self.run_stream_test(
                source.to_df(),
                AddData(source, 1),
                Assert(boom, "raises"),
            )
        msg = str(ctx.exception)
        self.assertIn("Assert raised", msg)
        self.assertIn("kaboom", msg)

    def test_assert_on_query_after_expect_failure(self):
        """After ExpectFailure, AssertOnQuery sees the just-failed query."""
        source = MemoryStream(self.spark, "int")
        df = source.to_df().selectExpr("raise_error('boom') as value")
        captured: List[str] = []

        def has_exception(q) -> bool:
            exc = q.exception()
            if exc is None:
                return False
            captured.append(str(exc))
            return True

        self.run_stream_test(
            df,
            AddData(source, 1),
            ExpectFailure(StreamingQueryException),
            AssertOnQuery(has_exception, "captured query failure"),
        )
        self.assertTrue(captured)
        self.assertIn("boom", captured[0])


# ---------------------------------------------------------------------------
# Verification actions: CheckAnswer / CheckLastBatch / CheckNewAnswer
# ---------------------------------------------------------------------------


_PERSON_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)


class CheckActionTests(StreamTest):
    """Tests for CheckAnswer / CheckLastBatch / CheckNewAnswer / CheckAnswerByFunc."""

    def test_passthrough_check_answer(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2, 3),
            CheckAnswer(Row(value=1), Row(value=2), Row(value=3)),
        )

    def test_simple_map_check_answer(self):
        source = MemoryStream(self.spark, "int")
        mapped = source.to_df().selectExpr("value + 1 as value")
        self.run_stream_test(
            mapped,
            AddData(source, 1, 2, 3),
            CheckAnswer(2, 3, 4),
        )

    def test_check_new_answer(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2),
            CheckNewAnswer(1, 2),
            AddData(source, 3),
            CheckNewAnswer(3),
        )

    def test_check_last_batch(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2),
            CheckLastBatch(1, 2),
            AddData(source, 3, 4),
            CheckLastBatch(3, 4),
        )

    def test_aggregation_complete_mode(self):
        source = MemoryStream(self.spark, "string")
        counts = source.to_df().groupBy("value").count()
        self.run_stream_test(
            counts,
            AddData(source, "a", "b", "a"),
            CheckAnswer(("a", 2), ("b", 1)),
            AddData(source, "b", "b"),
            CheckAnswer(("a", 2), ("b", 3)),
            output_mode="complete",
        )

    def test_filter(self):
        source = MemoryStream(self.spark, "int")
        filtered = source.to_df().filter("value > 2")
        self.run_stream_test(
            filtered,
            AddData(source, 1, 2, 3, 4, 5),
            CheckAnswer(3, 4, 5),
            AddData(source, 0, 10),
            CheckAnswer(3, 4, 5, 10),
        )

    def test_empty_check_answer(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            CheckAnswer(),  # nothing added yet
            AddData(source, 1),
            CheckAnswer(1),
        )

    def test_check_answer_by_func(self):
        source = MemoryStream(self.spark, "int")

        def check(rows):
            values = sorted(r.value for r in rows)
            assert values == [1, 2, 3], f"got {values}"

        self.run_stream_test(
            source.to_df(),
            AddData(source, 3, 1, 2),
            CheckAnswerByFunc(check),
        )

    def test_check_answer_by_func_last_only(self):
        source = MemoryStream(self.spark, "int")

        def only_one_batch(rows):
            assert len(rows) == 2, f"expected 2 rows in last batch, got {len(rows)}"

        self.run_stream_test(
            source.to_df(),
            AddData(source, 1),
            ProcessAllAvailable(),
            AddData(source, 2, 3),
            CheckAnswerByFunc(only_one_batch, last_only=True),
        )

    def test_stop_and_restart_check_answer(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2),
            CheckAnswer(1, 2),
            StopStream(),
            AddData(source, 3),
            StartStream(),
            CheckAnswer(1, 2, 3),
        )

    def test_check_answer_then_check_new_answer(self):
        """CheckAnswer must advance the cumulative cursor so a follow-up
        CheckNewAnswer only sees rows added after the CheckAnswer.

        Mirrors Scala's fetchStreamAnswer behavior, which advances
        lastFetchedMemorySinkLastBatchId on every check action.
        """
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2),
            CheckAnswer(1, 2),
            AddData(source, 3),
            CheckNewAnswer(3),
        )

    def test_check_last_batch_then_check_new_answer(self):
        """CheckLastBatch must advance the cumulative-row counter.

        Otherwise a follow-up CheckNewAnswer would re-include rows that
        were already validated by CheckLastBatch.
        """
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2),
            CheckLastBatch(1, 2),
            AddData(source, 3),
            CheckNewAnswer(3),
        )

    def test_check_last_batch_then_empty_check_new_answer(self):
        """CheckNewAnswer with no intervening AddData reports zero new rows."""
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2),
            CheckLastBatch(1, 2),
            CheckNewAnswer(),
        )

    def test_check_new_answer_across_restart(self):
        """``_last_fetched_rows`` must persist across StopStream+StartStream.

        After restart, the framework wipes the checkpoint dir and the
        ``MemoryStream`` source replays all batches. The new ``MemorySink``
        therefore exposes the original batch ids 0..N alongside the new
        batch N+1. ``CheckNewAnswer`` must only return rows from batches
        the user has not yet acknowledged.
        """
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2),
            CheckNewAnswer(1, 2),  # consumes batch 0
            StopStream(),
            AddData(source, 3),
            StartStream(),
            # The new sink replays batch 0 [1,2] and adds batch 1 [3]; we
            # expect only the rows from batch 1.
            CheckNewAnswer(3),
        )

    def test_check_last_batch_without_committed_batch_fails_clearly(self):
        source = MemoryStream(self.spark, "int")
        with self.assertRaises(AssertionError) as ctx:
            self.run_stream_test(
                source.to_df(),
                # No AddData -> no committed batch.
                CheckLastBatch(1),
            )
        self.assertIn("at least one committed batch", str(ctx.exception))


# ---------------------------------------------------------------------------
# Flexible-type CheckAnswer (mirrors Scala's ``CheckAnswer[A: Encoder]``)
# ---------------------------------------------------------------------------


class CheckAnswerFlexibleTypesTests(StreamTest):
    """Verify ``CheckAnswer`` accepts the same shapes as its Scala counterpart."""

    def test_scalars(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df().selectExpr("value + 1 as value"),
            AddData(source, 1, 2, 3),
            CheckAnswer(2, 3, 4),
        )

    def test_string_scalars(self):
        source = MemoryStream(self.spark, "string")
        self.run_stream_test(
            source.to_df(),
            AddData(source, "a", "b"),
            CheckAnswer("a", "b"),
        )

    def test_tuples(self):
        source = MemoryStream(self.spark, _PERSON_SCHEMA)
        self.run_stream_test(
            source.to_df(),
            AddData(source, ("Alice", 30), ("Bob", 25)),
            CheckAnswer(("Alice", 30), ("Bob", 25)),
        )

    def test_dicts(self):
        source = MemoryStream(self.spark, _PERSON_SCHEMA)
        self.run_stream_test(
            source.to_df(),
            AddData(source, {"name": "Alice", "age": 30}),
            CheckAnswer({"name": "Alice", "age": 30}),
        )

    def test_dataclass(self):
        @dataclass
        class Person:
            name: str
            age: int

        source = MemoryStream(self.spark, _PERSON_SCHEMA)
        self.run_stream_test(
            source.to_df(),
            AddData(source, Person("Alice", 30), Person("Bob", 25)),
            CheckAnswer(Person("Alice", 30), Person("Bob", 25)),
        )

    def test_namedtuple(self):
        PT = namedtuple("PT", ["name", "age"])
        source = MemoryStream(self.spark, _PERSON_SCHEMA)
        self.run_stream_test(
            source.to_df(),
            AddData(source, PT("Alice", 30)),
            CheckAnswer(PT("Alice", 30)),
        )

    def test_typed_namedtuple(self):
        class TP(NamedTuple):
            name: str
            age: int

        source = MemoryStream(self.spark, _PERSON_SCHEMA)
        self.run_stream_test(
            source.to_df(),
            AddData(source, TP("Charlie", 40)),
            CheckAnswer(TP("Charlie", 40)),
        )

    def test_plain_class(self):
        class Employee:
            def __init__(self, name, age):
                self.name = name
                self.age = age

        source = MemoryStream(self.spark, _PERSON_SCHEMA)
        self.run_stream_test(
            source.to_df(),
            AddData(source, Employee("Diana", 35)),
            CheckAnswer(Employee("Diana", 35)),
        )

    def test_mixed_types(self):
        source = MemoryStream(self.spark, "int")
        self.run_stream_test(
            source.to_df(),
            AddData(source, 1, 2, 3),
            CheckAnswer(Row(value=1), 2, 3),  # mix Row + scalar
        )

    def test_aggregation_with_tuples(self):
        source = MemoryStream(self.spark, "string")
        counts = source.to_df().groupBy("value").count()
        self.run_stream_test(
            counts,
            AddData(source, "a", "b", "a"),
            CheckAnswer(("a", 2), ("b", 1)),
            output_mode="complete",
        )


# ---------------------------------------------------------------------------
# Negative cases: error messages on Check failures
# ---------------------------------------------------------------------------


class CheckActionErrorReportingTests(StreamTest):

    def test_check_answer_mismatch_includes_diagnostic(self):
        source = MemoryStream(self.spark, "int")
        with self.assertRaises(AssertionError) as ctx:
            self.run_stream_test(
                source.to_df(),
                AddData(source, 1, 2),
                CheckAnswer(99),
            )
        msg = str(ctx.exception)
        self.assertIn("Row mismatch", msg)
        self.assertIn("Missing rows", msg)
        self.assertIn("Unexpected rows", msg)
        # The progress trace should mark the failing action.
        self.assertIn("=> CheckAnswer", msg)


# ---------------------------------------------------------------------------
# Real-Time Mode (RTM)
# ---------------------------------------------------------------------------


class RtmTests(StreamTest):
    """Tests covering ``LowLatencyMemoryStream`` + ``ContinuousMemorySink`` + RTM trigger."""

    streaming_timeout_seconds = 30.0
    # Allow sub-5-second RTM batch durations for fast tests. The default
    # production minimum is 5000 ms which would make these tests slow.
    spark_conf = {"spark.sql.streaming.realTimeMode.minBatchDuration": "100"}

    def test_low_latency_memory_stream_smoke(self):
        """LowLatencyMemoryStream + ContinuousMemorySink + Trigger.RealTime end-to-end.

        Uses the polling Check action (RTM batches run on wall-clock
        intervals; processAllAvailable is a no-op).
        """
        source = LowLatencyMemoryStream(self.spark, "int", num_partitions=2)
        sink = ContinuousMemorySink(self.spark)
        df = source.to_df()
        self.run_stream_test(
            df,
            StartStream(trigger={"realTime": "200 milliseconds"}),
            AddData(source, 1, 2, 3),
            CheckAnswerWithTimeout(10_000, 1, 2, 3),
            output_mode="update",
            sink=sink,
        )

    def test_check_answer_contains_with_timeout(self):
        """Subset check: extra rows in the sink are tolerated."""
        source = LowLatencyMemoryStream(self.spark, "int", num_partitions=1)
        sink = ContinuousMemorySink(self.spark)
        self.run_stream_test(
            source.to_df(),
            StartStream(trigger={"realTime": "200 milliseconds"}),
            AddData(source, 1, 2, 3, 4, 5),
            CheckAnswerContainsWithTimeout(10_000, 2, 4),
            output_mode="update",
            sink=sink,
        )

    def test_realtime_trigger_requires_custom_sink(self):
        """``trigger={"realTime": ...}`` without a ``sink=`` is a clear error.

        The driver wraps the validation error in an AssertionError with
        the action progress trace, so the test checks for the wrapped form.
        """
        source = LowLatencyMemoryStream(self.spark, "int")
        with self.assertRaises(AssertionError) as ctx:
            self.run_stream_test(
                source.to_df(),
                StartStream(trigger={"realTime": "200 milliseconds"}),
                AddData(source, 1),
                ProcessAllAvailable(),
            )
        self.assertIn("ContinuousMemorySink", str(ctx.exception))

    def test_external_action_runs(self):
        captured: List[str] = []
        source = LowLatencyMemoryStream(self.spark, "int", num_partitions=1)
        sink = ContinuousMemorySink(self.spark)
        self.run_stream_test(
            source.to_df(),
            StartStream(trigger={"realTime": "200 milliseconds"}),
            AddData(source, 1),
            ExternalAction(lambda: captured.append("ran"), name="capture"),
            CheckAnswerWithTimeout(10_000, 1),
            output_mode="update",
            sink=sink,
        )
        self.assertEqual(captured, ["ran"])

    def test_sleep_action(self):
        source = LowLatencyMemoryStream(self.spark, "int", num_partitions=1)
        sink = ContinuousMemorySink(self.spark)
        self.run_stream_test(
            source.to_df(),
            StartStream(trigger={"realTime": "100 milliseconds"}),
            AddData(source, 1),
            Sleep(0.2),
            CheckAnswerWithTimeout(10_000, 1),
            output_mode="update",
            sink=sink,
        )

    def test_low_latency_memory_stream_rejects_zero_partitions(self):
        with self.assertRaises(ValueError):
            LowLatencyMemoryStream(self.spark, "int", num_partitions=0)

    def test_check_answer_with_timeout_validates_timeout(self):
        with self.assertRaises(ValueError):
            CheckAnswerWithTimeout(0, 1)

    def test_sleep_validates_seconds(self):
        with self.assertRaises(ValueError):
            Sleep(-1)

    def test_check_answer_with_timeout_rejects_empty(self):
        with self.assertRaises(ValueError):
            CheckAnswerWithTimeout(1000)

    def test_check_answer_contains_with_timeout_rejects_empty(self):
        with self.assertRaises(ValueError):
            CheckAnswerContainsWithTimeout(1000)

    def test_check_last_batch_rejected_with_custom_sink(self):
        """CheckLastBatch on the custom-sink path must give a clear framework
        error rather than crashing inside ContinuousMemorySink.dataSinceBatch."""
        source = LowLatencyMemoryStream(self.spark, "int", num_partitions=1)
        sink = ContinuousMemorySink(self.spark)
        with self.assertRaises(AssertionError) as ctx:
            self.run_stream_test(
                source.to_df(),
                StartStream(trigger={"realTime": "200 milliseconds"}),
                AddData(source, 1),
                CheckLastBatch(1),
                output_mode="update",
                sink=sink,
            )
        self.assertIn("not supported with a custom sink", str(ctx.exception))

    def test_check_new_answer_rejected_with_custom_sink(self):
        """CheckNewAnswer on the custom-sink path must give a clear framework error."""
        source = LowLatencyMemoryStream(self.spark, "int", num_partitions=1)
        sink = ContinuousMemorySink(self.spark)
        with self.assertRaises(AssertionError) as ctx:
            self.run_stream_test(
                source.to_df(),
                StartStream(trigger={"realTime": "200 milliseconds"}),
                AddData(source, 1),
                CheckNewAnswer(1),
                output_mode="update",
                sink=sink,
            )
        self.assertIn("not supported with a custom sink", str(ctx.exception))

    def test_sink_cleared_on_reuse(self):
        """Reusing the same ContinuousMemorySink across run_stream_test
        invocations must not bleed rows from the prior run."""
        source1 = LowLatencyMemoryStream(self.spark, "int", num_partitions=1)
        source2 = LowLatencyMemoryStream(self.spark, "int", num_partitions=1)
        sink = ContinuousMemorySink(self.spark)
        self.run_stream_test(
            source1.to_df(),
            StartStream(trigger={"realTime": "200 milliseconds"}),
            AddData(source1, 100, 200),
            CheckAnswerWithTimeout(10_000, 100, 200),
            output_mode="update",
            sink=sink,
        )
        # Without clear() on the second run, the new sink would still hold
        # 100 and 200 from the first run and CheckAnswerWithTimeout(1, 2)
        # would fail with "unexpected rows: 100, 200".
        self.run_stream_test(
            source2.to_df(),
            StartStream(trigger={"realTime": "200 milliseconds"}),
            AddData(source2, 1, 2),
            CheckAnswerWithTimeout(10_000, 1, 2),
            output_mode="update",
            sink=sink,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
