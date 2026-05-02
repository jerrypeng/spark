#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Real-Time Mode (RTM) helpers for the PySpark ``StreamTest`` framework.

Provides Python wrappers for the JVM-side ``LowLatencyMemoryStream`` and
``ContinuousMemorySink`` so RTM-triggered streaming queries can be driven
from Python tests. Pair with ``trigger={"realTime": "<duration>"}`` and a
``ContinuousMemorySink`` passed to ``StreamTest.run_stream_test(sink=...)``.

RTM tests in Scala (see
``sql/core/src/test/scala/org/apache/spark/sql/streaming/StreamRealTimeModeSuite.scala``)
attach the sink directly via ``testStream(df, mode, opts, sink)(...)``;
Python mirrors that path through the
``PythonSQLUtils.startStreamingQueryWithSink`` JVM bridge.
"""

from __future__ import annotations

from typing import Any, List, Optional, Union

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructType
from pyspark.testing.streaming._conversions import resolve_schema, to_rows


class LowLatencyMemoryStream:
    """A controllable RTM streaming source backed by the JVM
    ``LowLatencyMemoryStream``.

    Mirrors :class:`pyspark.testing.streaming.MemoryStream` but produces a
    source compatible with ``Trigger.RealTime`` queries. Required when the
    test sets ``trigger={"realTime": "<duration>"}``: a regular
    ``MemoryStream`` does not implement ``SupportsRealTimeMode`` and would
    raise on the JVM side at planning time.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession (JVM-backed; Spark Connect not supported).
    schema : str or StructType, default ``"int"``
        Schema spec; same shorthand rules as ``MemoryStream``.
    num_partitions : int, default 2
        Number of source partitions. RTM splits added rows across these.
    """

    def __init__(
        self,
        spark: SparkSession,
        schema: Union[str, StructType] = "int",
        num_partitions: int = 2,
    ) -> None:
        if num_partitions < 1:
            raise ValueError(f"num_partitions must be >= 1, got {num_partitions}")
        self._spark = spark
        self._schema = resolve_schema(schema)
        self._num_partitions = int(num_partitions)
        jvm = self._jvm()
        jspark = spark._jsparkSession  # type: ignore[attr-defined]
        jschema = jspark.parseDataType(self._schema.json())
        py_utils = jvm.org.apache.spark.sql.api.python.PythonSQLUtils
        self._jstream = py_utils.createLowLatencyMemoryStream(
            jspark, jschema, self._num_partitions
        )

    def _jvm(self) -> Any:
        jvm = getattr(self._spark, "_jvm", None)
        if jvm is None:
            raise RuntimeError(
                "LowLatencyMemoryStream requires a JVM-backed SparkSession; "
                "Spark Connect sessions are not supported."
            )
        return jvm

    @property
    def schema(self) -> StructType:
        return self._schema

    @property
    def num_partitions(self) -> int:
        return self._num_partitions

    def add_data(self, *rows: Any) -> None:
        """Append rows to the underlying JVM low-latency memory stream.

        Unlike ``MemoryStream``, RTM does not surface a stable batch offset
        per call, so this method does not return one.
        """
        if not rows:
            return
        converted = to_rows(list(rows), self._schema)
        df = self._spark.createDataFrame(converted, schema=self._schema)
        py_utils = self._jvm().org.apache.spark.sql.api.python.PythonSQLUtils
        py_utils.lowLatencyMemoryStreamAddData(self._jstream, df._jdf)

    def to_df(self) -> DataFrame:
        """Return a streaming ``DataFrame`` reading from this RTM source."""
        py_utils = self._jvm().org.apache.spark.sql.api.python.PythonSQLUtils
        jdf = py_utils.lowLatencyMemoryStreamToDF(self._jstream)
        return DataFrame(jdf, self._spark)

    def __repr__(self) -> str:
        return (
            f"LowLatencyMemoryStream(schema={self._schema.simpleString()}, "
            f"num_partitions={self._num_partitions})"
        )


class ContinuousMemorySink:
    """A Python wrapper around the JVM ``ContinuousMemorySink``.

    RTM queries cannot use the standard ``writeStream.format("memory")``
    sink (which buffers per micro-batch); the continuous sink is required.
    Pass an instance to ``StreamTest.run_stream_test(sink=...)``: the driver
    will use the ``startStreamingQueryWithSink`` JVM bridge instead of the
    ``format("memory")`` route.

    The framework automatically calls :meth:`set_schema` with the streaming
    DataFrame's output schema before starting the query, so
    :meth:`all_data` can return rows with the right column names.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        jvm = self._jvm()
        py_utils = jvm.org.apache.spark.sql.api.python.PythonSQLUtils
        self._jsink = py_utils.createContinuousMemorySink()
        self._schema: Optional[StructType] = None
        self._jschema: Optional[Any] = None

    def _jvm(self) -> Any:
        jvm = getattr(self._spark, "_jvm", None)
        if jvm is None:
            raise RuntimeError(
                "ContinuousMemorySink requires a JVM-backed SparkSession; "
                "Spark Connect sessions are not supported."
            )
        return jvm

    def set_schema(self, schema: StructType) -> None:
        """Set the output schema so :meth:`all_data` returns named-field rows."""
        self._schema = schema
        self._jschema = self._spark._jsparkSession.parseDataType(schema.json())

    def all_data(self) -> List[Row]:
        """Return all rows currently in the continuous memory sink.

        Raises ``RuntimeError`` if :meth:`set_schema` has not been called
        yet. The ``StreamTest`` driver always calls ``set_schema`` before
        any sink read, so this only fires when the sink is inspected
        directly (outside the framework) before being bound to a query.
        """
        if self._schema is None:
            raise RuntimeError(
                "ContinuousMemorySink.all_data() called before set_schema(). "
                "When using StreamTest, attach the sink via "
                "run_stream_test(sink=...) so the framework can set the "
                "output schema; otherwise call set_schema(...) yourself."
            )
        # Read directly from the JVM sink via the dedicated helper that
        # accepts a sink object (rather than the streaming query) -- needed
        # because the StreamTest driver attaches the sink before any
        # query exists.
        py_utils = self._jvm().org.apache.spark.sql.api.python.PythonSQLUtils
        jdf = py_utils.continuousMemorySinkAllData(
            self._jsink, self._jschema, self._spark._jsparkSession
        )
        return DataFrame(jdf, self._spark).collect()

    def clear(self) -> None:
        """Drop all accumulated rows from the JVM sink.

        Useful when the same sink is reused across ``run_stream_test``
        invocations; the framework calls this automatically at the start
        of each run.
        """
        self._jsink.clear()

    def __repr__(self) -> str:
        return "ContinuousMemorySink()"


__all__ = ["LowLatencyMemoryStream", "ContinuousMemorySink"]
