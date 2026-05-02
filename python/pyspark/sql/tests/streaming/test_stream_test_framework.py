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
from typing import Any, List

from pyspark.errors import StreamingQueryException
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.streaming import (
    AddData,
    Assert,
    AssertOnQuery,
    Execute,
    ExpectFailure,
    MemoryStream,
    ProcessAllAvailable,
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


if __name__ == "__main__":
    from pyspark.testing import main

    main()
