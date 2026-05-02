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
"""``StreamTest`` -- a declarative, action-based base class for testing
PySpark Structured Streaming queries from Python.

Inspired by Scala's ``StreamTest`` trait (see
``sql/core/src/test/scala/org/apache/spark/sql/streaming/StreamTest.scala``).
The framework runs a streaming DataFrame against a memory sink and walks a
user-supplied sequence of ``StreamAction`` objects (defined in
:mod:`pyspark.testing.streaming.actions`), blocking on data availability
where required and producing detailed error messages on failure.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
import time
import traceback
import unittest
from collections import Counter
from typing import Any, Dict, List, NoReturn, Optional

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructType
from pyspark.testing.streaming.actions import (
    AddData,
    Assert,
    AssertOnQuery,
    CheckAnswer,
    CheckAnswerByFunc,
    CheckAnswerContainsWithTimeout,
    CheckAnswerWithTimeout,
    CheckLastBatch,
    CheckNewAnswer,
    Execute,
    ExpectFailure,
    ExternalAction,
    ProcessAllAvailable,
    StartStream,
    StopStream,
    StreamAction,
    _CheckActionBase,
)

logger = logging.getLogger(__name__)


class StreamTest(unittest.TestCase):
    """``unittest`` base class for declarative streaming tests.

    Subclasses inherit a ``self.spark`` SparkSession (configured for fast,
    deterministic streaming) and a ``run_stream_test()`` method that walks
    a list of ``StreamAction`` objects against a streaming DataFrame.

    Class-level attributes that subclasses may override:

    - ``streaming_timeout_seconds`` -- wall-clock cap for each blocking step.
    - ``default_trigger`` -- trigger applied when ``StartStream`` does not
      specify one. Defaults to ``processingTime=0 seconds``.
    - ``spark_conf`` -- extra ``SparkSession`` configs merged on top of the
      framework defaults.
    """

    streaming_timeout_seconds: float = 60.0
    default_trigger: Optional[Dict[str, Any]] = {"processingTime": "0 seconds"}
    spark_conf: Optional[Dict[str, str]] = None

    _spark: Optional[SparkSession] = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        builder = (
            SparkSession.builder.appName(cls.__name__)
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.streaming.schemaInference", "true")
        )
        if cls.spark_conf:
            for k, v in cls.spark_conf.items():
                builder = builder.config(k, v)
        cls._spark = builder.getOrCreate()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if cls._spark is not None:
                for q in cls._spark.streams.active:
                    try:
                        q.stop()
                        # Bound the wait so a hung query can't deadlock
                        # the test runner, but do wait so we don't race
                        # the next class's session setup.
                        if not q.awaitTermination(cls.streaming_timeout_seconds):
                            logger.warning(
                                "Streaming query %r did not terminate within %.1fs",
                                getattr(q, "name", "<unknown>"),
                                cls.streaming_timeout_seconds,
                            )
                    except Exception:
                        # Best-effort teardown; failures here would mask
                        # the real test failure.
                        logger.exception("Failed to stop active query during teardown")
                cls._spark.stop()
                cls._spark = None
        finally:
            super().tearDownClass()

    @property
    def spark(self) -> SparkSession:
        assert self._spark is not None, "SparkSession not initialized"
        return self._spark

    # ------------------------------------------------------------------
    # Public driver
    # ------------------------------------------------------------------

    def run_stream_test(
        self,
        stream: DataFrame,
        *actions: StreamAction,
        output_mode: str = "append",
        extra_options: Optional[Dict[str, str]] = None,
        sink: Optional[Any] = None,
    ) -> None:
        """Execute ``actions`` against the streaming DataFrame ``stream``.

        Drives a ``writeStream.format("memory")`` query (or, when ``sink`` is
        provided, an explicit-sink query started via the JVM
        ``startStreamingQueryWithSink`` helper) and walks the action list
        synchronously. Auto-inserts ``StartStream`` if the sequence does not
        begin with one and the first action that needs a running stream
        comes before any explicit start.

        Parameters
        ----------
        stream
            The streaming DataFrame under test.
        *actions
            ``StreamAction`` objects to execute in order.
        output_mode
            ``"append"`` (default), ``"complete"`` or ``"update"``.
        extra_options
            Extra options applied to every ``writeStream`` invocation.
        sink
            Optional custom sink. Pass a
            :class:`pyspark.testing.streaming.rtm.ContinuousMemorySink` for
            RTM tests; the driver will route Check actions through the sink
            object directly instead of through the memory format table.
        """
        if not stream.isStreaming:
            raise ValueError(
                "run_stream_test requires a streaming DataFrame; got a batch "
                "DataFrame. Build one with MemoryStream.to_df() or "
                "spark.readStream..."
            )
        runner = _Runner(
            self, stream, list(actions), output_mode, extra_options or {}, sink
        )
        runner.run()


# ---------------------------------------------------------------------------
# Driver implementation
# ---------------------------------------------------------------------------


# Names that are safe to embed in a SQL identifier without quoting.
_SAFE_IDENT_RE = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")


class _MemorySinkReader:
    """Reads rows from the JVM ``MemorySink`` of a streaming query.

    Goes through ``PythonSQLUtils`` helpers rather than ``SELECT * FROM
    <queryName>`` so that batch arrival order is preserved (the SQL path
    provides no ordering guarantees, which breaks per-batch row slicing).

    Each instance is bound to a specific running query; the JVM query
    handle is updated whenever the framework starts a new query.
    """

    def __init__(self, spark: SparkSession, output_schema: StructType) -> None:
        self._spark = spark
        self._schema = output_schema
        self._jschema = spark._jsparkSession.parseDataType(output_schema.json())
        self._jvm = spark._jvm
        self._jquery: Optional[Any] = None

    def attach(self, jquery: Any) -> None:
        """Bind to the JVM ``StreamingQuery`` handle."""
        self._jquery = jquery

    def detach(self) -> None:
        self._jquery = None

    def _utils(self) -> Any:
        if self._jquery is None:
            raise RuntimeError("MemorySinkReader is not attached to any query")
        return self._jvm.org.apache.spark.sql.api.python.PythonSQLUtils

    def all_data(self) -> List[Row]:
        utils = self._utils()
        jdf = utils.memorySinkAllData(
            self._jquery, self._jschema, self._spark._jsparkSession
        )
        return DataFrame(jdf, self._spark).collect()

    def data_since_batch(self, since_batch_id: int) -> List[Row]:
        utils = self._utils()
        jdf = utils.memorySinkDataSinceBatch(
            self._jquery, int(since_batch_id), self._jschema, self._spark._jsparkSession
        )
        return DataFrame(jdf, self._spark).collect()

    def latest_batch_id(self) -> Optional[int]:
        utils = self._utils()
        jval = utils.memorySinkLatestBatchId(self._jquery)
        return None if jval is None else int(jval)


def _format_rows(rows: List[Row], limit: int = 25) -> str:
    """Pretty-print up to ``limit`` rows for error messages."""
    if not rows:
        return "  <no rows>"
    shown = rows[:limit]
    suffix = f"\n  ... ({len(rows) - limit} more)" if len(rows) > limit else ""
    return "\n".join(f"  {r}" for r in shown) + suffix


def _diff_rows(
    expected: List[Row],
    actual: List[Row],
    ordered: bool,
) -> Optional[str]:
    """Compare ``expected`` and ``actual``. Return None on match, else error.

    Uses Row-level equality (via ``Row.__eq__``) rather than ``str(...)`` so
    that Counter keys are tied to the actual data identity. ``Counter`` keys
    must be hashable; ``Row`` is a tuple subclass and is hashable as long as
    its values are hashable. For values that aren't hashable (e.g. ``list``,
    ``dict``), we fall back to a quadratic ``Row.__eq__`` based comparison so
    error messages stay row-shaped instead of a stringified Counter dict.
    """
    if ordered:
        if expected == actual:
            return None
        return (
            f"Rows not equal (ordered comparison).\n"
            f"Expected ({len(expected)} rows):\n{_format_rows(expected)}\n"
            f"Actual ({len(actual)} rows):\n{_format_rows(actual)}"
        )
    try:
        e_counter: Counter = Counter(expected)
        a_counter: Counter = Counter(actual)
        if e_counter == a_counter:
            return None
        missing_rows = list((e_counter - a_counter).elements())
        extra_rows = list((a_counter - e_counter).elements())
    except TypeError:
        # Unhashable contents -- multiset diff via O(n*m) Row.__eq__.
        actual_remaining = list(actual)
        missing_rows = []
        for row in expected:
            try:
                actual_remaining.remove(row)
            except ValueError:
                missing_rows.append(row)
        extra_rows = actual_remaining
    if not missing_rows and not extra_rows:
        return None
    parts: List[str] = []
    if missing_rows:
        parts.append(f"Missing rows ({len(missing_rows)}):\n{_format_rows(missing_rows)}")
    if extra_rows:
        parts.append(f"Unexpected rows ({len(extra_rows)}):\n{_format_rows(extra_rows)}")
    return (
        f"Row mismatch.\n"
        f"Expected ({len(expected)} rows):\n{_format_rows(expected)}\n"
        f"Actual ({len(actual)} rows):\n{_format_rows(actual)}\n"
        + "\n".join(parts)
    )


class _Runner:
    """Single-use ``run_stream_test`` driver.

    Encapsulating per-invocation state in a class keeps the bookkeeping
    explicit (instead of nested closures over mutable locals) and makes
    error messages easier to assemble.
    """

    def __init__(
        self,
        test: StreamTest,
        stream: DataFrame,
        actions: List[StreamAction],
        output_mode: str,
        extra_options: Dict[str, str],
        custom_sink: Optional[Any] = None,
    ) -> None:
        self.test = test
        self.stream = stream
        self.output_mode = output_mode
        self.extra_options = extra_options
        self.output_schema: StructType = stream.schema
        # When set, query starts via ``startStreamingQueryWithSink`` and Check
        # actions read directly from the supplied sink object instead of from
        # the format("memory") table. Required for RTM tests.
        self.custom_sink: Optional[Any] = custom_sink
        if self.custom_sink is not None:
            if hasattr(self.custom_sink, "set_schema"):
                self.custom_sink.set_schema(self.output_schema)
            # Wipe any rows left over from a prior ``run_stream_test`` so
            # tests that reuse a sink (e.g. across helper methods) don't
            # see stale data.
            if hasattr(self.custom_sink, "clear"):
                try:
                    self.custom_sink.clear()
                except Exception:
                    logger.exception(
                        "Failed to clear custom sink before run_stream_test; "
                        "stale rows may bleed into this test"
                    )

        # A per-test query name so multiple tests can run with disjoint
        # memory-sink tables. The pid + monotonic ns ensure uniqueness
        # across processes and rapid succession.
        self.query_name = f"stream_test_{os.getpid()}_{time.monotonic_ns()}"
        if not _SAFE_IDENT_RE.match(self.query_name):
            raise RuntimeError(f"Generated unsafe query name: {self.query_name!r}")
        self.checkpoint_dir: str = tempfile.mkdtemp(prefix="stream_test_checkpoint_")
        self.sink_reader = _MemorySinkReader(self.test.spark, self.output_schema)

        self.current_query: Optional[Any] = None
        self.last_query: Optional[Any] = None
        # Number of rows already consumed by ``CheckNewAnswer`` / a prior
        # ``_fetch_new`` call. Drives the slice ``all_data[_last_fetched_rows:]``
        # for ``CheckNewAnswer``. We track row count rather than sink batch id
        # because batch ids are not stable across StopStream + StartStream
        # (the new sink may compress replayed source batches into one).
        # ``MemorySink.allData`` is collected in batch arrival order on the
        # JVM side, so positional slicing is sound.
        self._last_fetched_rows: int = 0
        self.actions: List[StreamAction] = self._maybe_auto_start(actions)
        self.pos = 0

    def _maybe_auto_start(
        self, actions: List[StreamAction]
    ) -> List[StreamAction]:
        for a in actions:
            if isinstance(a, StartStream):
                return actions
            if a.requires_running_stream:
                return [StartStream(), *actions]
        return actions

    # ----- error reporting ---------------------------------------------

    def _state_dump(self) -> str:
        lines = ["", "== Progress =="]
        for i, a in enumerate(self.actions):
            marker = "=> " if i == self.pos else "   "
            lines.append(f"{marker}{a}")
        lines.append("")
        lines.append("== Stream ==")
        lines.append(f"output mode: {self.output_mode}")
        # Prefer the running query, but fall back to last_query so failures
        # in actions that run after StopStream / ExpectFailure (e.g.
        # AssertOnQuery, Execute, Assert) still surface the captured query
        # exception and the just-stopped query state.
        query = self.current_query if self.current_query is not None else self.last_query
        query_label = "current" if self.current_query is not None else "last"
        if query is not None:
            try:
                lines.append(f"{query_label} query active: {bool(query.isActive)}")
            except Exception:
                lines.append(f"{query_label} query active: <unknown>")
            try:
                exc = query.exception()
                if exc is not None:
                    lines.append(f"{query_label} query exception: {exc}")
            except Exception:
                pass
        else:
            lines.append("query active: no")
        lines.append(f"checkpoint: {self.checkpoint_dir}")
        return "\n".join(lines)

    def _fail(self, message: str, cause: Optional[BaseException] = None) -> NoReturn:
        full = f"\n{message}\n{self._state_dump()}"
        if cause is not None:
            full += f"\nCaused by: {cause!r}\n{traceback.format_exc()}"
        raise AssertionError(full)

    def _verify(self, condition: bool, message: str) -> None:
        if not condition:
            self._fail(message)

    # ----- trigger / custom-sink helpers -------------------------------

    # Trigger keys this framework recognizes. Anything else is rejected
    # explicitly so a misspelling or an unsupported feature (e.g.
    # ``continuous=...``) does not silently fall through to the
    # ProcessingTime(0) default or ``DataStreamWriter.trigger``'s own
    # less-friendly TypeError.
    _SUPPORTED_TRIGGER_KEYS = frozenset(
        {"realTime", "processingTime", "once", "availableNow"}
    )

    @classmethod
    def _writer_trigger_kwargs(cls, trigger: Dict[str, Any]) -> Dict[str, Any]:
        """Translate the framework's trigger dict into ``DataStreamWriter.trigger``
        keyword arguments. The ``realTime`` key is RTM-specific and must be
        applied through the explicit-sink path; reject it here so users get a
        clear error if they try to use it without a custom sink. Other
        validation (unknown keys, multiple keys, falsy flag values) is shared
        with the custom-sink path via :meth:`_validate_trigger_keys`.
        """
        if "realTime" in trigger:
            raise ValueError(
                "trigger={'realTime': ...} requires sink= to be a "
                "ContinuousMemorySink (or another RTM-compatible sink). "
                "RTM cannot use the standard format(\"memory\") sink."
            )
        cls._validate_trigger_keys(trigger)
        return dict(trigger)

    def _jvm_trigger(self, trigger: Optional[Dict[str, Any]]) -> Any:
        """Build a JVM ``Trigger`` from the framework's trigger dict for the
        explicit-sink (RTM) path. Defaults to ``ProcessingTime(0)`` when no
        trigger is supplied. Rejects unknown trigger keys, multiple keys,
        and falsy values for ``once`` / ``availableNow``."""
        jvm = self.test.spark._jvm  # type: ignore[attr-defined]
        if trigger:
            self._validate_trigger_keys(trigger)
        if trigger and "realTime" in trigger:
            duration = trigger["realTime"]
            py_utils = jvm.org.apache.spark.sql.api.python.PythonSQLUtils
            # Delegate duration parsing to the JVM so we accept exactly the
            # same shapes as Scala's ``RealTimeTrigger.apply``: int (ms)
            # or string ("4 seconds", "200 ms", ...).
            if isinstance(duration, bool):  # bool is an int subclass
                raise TypeError(
                    f"realTime duration must be int or str, got bool: {duration!r}"
                )
            if isinstance(duration, int):
                if duration <= 0:
                    raise ValueError(
                        f"realTime duration must be > 0 ms, got {duration}"
                    )
                return py_utils.realTimeTrigger(int(duration))
            if isinstance(duration, str):
                return py_utils.realTimeTriggerFromString(duration)
            raise TypeError(
                f"realTime duration must be int or str, got "
                f"{type(duration).__name__}"
            )
        if trigger and "processingTime" in trigger:
            return jvm.org.apache.spark.sql.streaming.Trigger.ProcessingTime(
                trigger["processingTime"]
            )
        if trigger and "once" in trigger:
            return jvm.org.apache.spark.sql.streaming.Trigger.Once()
        if trigger and "availableNow" in trigger:
            return jvm.org.apache.spark.sql.streaming.Trigger.AvailableNow()
        return jvm.org.apache.spark.sql.streaming.Trigger.ProcessingTime("0 seconds")

    @classmethod
    def _validate_trigger_keys(cls, trigger: Dict[str, Any]) -> None:
        """Reject unknown keys, multiple keys, and falsy ``once`` /
        ``availableNow`` values. Shared by the standard (writer) and
        custom-sink (RTM) trigger paths."""
        unknown = set(trigger).difference(cls._SUPPORTED_TRIGGER_KEYS)
        if unknown:
            raise ValueError(
                f"Unsupported trigger keys: {sorted(unknown)}. "
                f"Supported: {sorted(cls._SUPPORTED_TRIGGER_KEYS)}."
            )
        present = sorted(set(trigger).intersection(cls._SUPPORTED_TRIGGER_KEYS))
        if len(present) > 1:
            raise ValueError(
                f"Exactly one trigger key is allowed; got {present}."
            )
        # ``once`` / ``availableNow`` are flag triggers; the value must
        # be exactly True. Falsy values (None, False, 0, "") would
        # otherwise be silently demoted to the ProcessingTime(0) default.
        for flag in ("once", "availableNow"):
            if flag in trigger and trigger[flag] is not True:
                raise ValueError(
                    f"trigger[{flag!r}] must be exactly True, got {trigger[flag]!r}"
                )

    def _start_with_custom_sink(
        self,
        trigger: Optional[Dict[str, Any]],
        checkpoint: str,
        opts: Dict[str, str],
    ) -> Any:
        spark = self.test.spark
        jvm = spark._jvm  # type: ignore[attr-defined]
        py_utils = jvm.org.apache.spark.sql.api.python.PythonSQLUtils
        jopts = jvm.java.util.HashMap()
        for k, v in opts.items():
            jopts.put(k, v)
        return py_utils.startStreamingQueryWithSink(
            spark._jsparkSession,  # type: ignore[attr-defined]
            self.stream._jdf,  # type: ignore[attr-defined]
            self.custom_sink._jsink,  # type: ignore[attr-defined]
            self.output_mode,
            checkpoint,
            self._jvm_trigger(trigger),
            jopts,
        )

    # ----- query lifecycle ---------------------------------------------

    def _start_query(
        self,
        trigger: Optional[Dict[str, Any]],
        checkpoint_override: Optional[str],
        opts: Optional[Dict[str, str]],
    ) -> None:
        self._verify(
            self.current_query is None or not self.current_query.isActive,
            "Cannot start stream: a query is already running",
        )

        chk = checkpoint_override or self.checkpoint_dir
        # MemoryStream cannot recover from a checkpoint -- its in-memory
        # offsets vanish on restart. Wipe the checkpoint dir on every start
        # so stop/restart sequences work. We only do this for the
        # framework-managed default checkpoint, never for an
        # explicitly-supplied path.
        if checkpoint_override is None and os.path.exists(chk):
            shutil.rmtree(chk, ignore_errors=True)
            os.makedirs(chk, exist_ok=True)

        previous_query = self.current_query

        trigger_conf = trigger or self.test.default_trigger
        merged_opts = {**self.extra_options, **(opts or {})}

        if self.custom_sink is not None:
            # RTM / custom-sink path: route through the JVM
            # ``startStreamingQueryWithSink`` helper so the test-supplied
            # sink instance is wired to the streaming query.
            jquery = self._start_with_custom_sink(trigger_conf, chk, merged_opts)
            from pyspark.sql.streaming.query import StreamingQuery

            # Defer the ``last_query`` swap until after the JVM call
            # returns successfully (see PR 2 commit message for context).
            self.current_query = StreamingQuery(jquery)
        else:
            writer = (
                self.stream.writeStream.outputMode(self.output_mode)
                .format("memory")
                .queryName(self.query_name)
                .option("checkpointLocation", chk)
            )
            for k, v in merged_opts.items():
                writer = writer.option(k, v)
            if trigger_conf:
                writer = writer.trigger(**self._writer_trigger_kwargs(trigger_conf))
            self.current_query = writer.start()
        if previous_query is not None:
            self.last_query = previous_query
        # Note: do NOT reset ``_last_fetched_rows`` unconditionally here. For
        # the supported "append mode + single MemoryStream + framework-managed
        # checkpoint wipe" configuration, the new query replays all source
        # batches and the new sink converges to the same cumulative row count
        # plus any new rows added between stop and start, so preserving the
        # count gives ``CheckNewAnswer`` the correct semantics across restart.
        # Complete-mode aggregations and update-mode queries do not have
        # stable row counts across replay; reset for those modes so post-
        # restart Check actions work against the new sink from scratch.
        if self.output_mode != "append":
            self._last_fetched_rows = 0
        if self.custom_sink is None:
            self.sink_reader.attach(
                self.current_query._jsq  # type: ignore[attr-defined]
            )


        # Wait briefly for the query thread to come up. If it dies before
        # initialization (e.g. analyzer error), surface that quickly.
        deadline = time.monotonic() + self.test.streaming_timeout_seconds
        while time.monotonic() < deadline:
            if self.current_query.isActive:
                return
            exc = self.current_query.exception()
            if exc is not None:
                # Don't fail here -- the user may have an ExpectFailure
                # later. Just stop polling.
                return
            time.sleep(0.05)
        self._fail(
            "Timed out waiting for query to become active "
            f"after {self.test.streaming_timeout_seconds:.1f}s"
        )

    def _stop_query(self) -> None:
        self._verify(self.current_query is not None, "Cannot stop: no running stream")
        try:
            self.current_query.stop()
            self.current_query.awaitTermination(self.test.streaming_timeout_seconds)
            self._verify(
                not self.current_query.isActive,
                "Query still active after stop()",
            )
            exc = self.current_query.exception()
            self._verify(
                exc is None,
                f"Query had exception during clean stop: {exc}",
            )
        except AssertionError:
            raise
        except BaseException as e:  # noqa: BLE001
            self._fail("Error while stopping stream", e)
        finally:
            self.last_query = self.current_query
            self.current_query = None
            self.sink_reader.detach()

    def _wait_for_processing(self) -> None:
        if self.current_query is None:
            return
        try:
            self.current_query.processAllAvailable()
        except BaseException as e:  # noqa: BLE001
            self._fail("Error waiting for stream processing", e)

    def _fetch_all(self) -> List[Row]:
        self._verify(self.current_query is not None, "Stream not running")
        self._wait_for_processing()
        if self.custom_sink is not None:
            rows = self.custom_sink.all_data()
        else:
            rows = self.sink_reader.all_data()
        # Advance the cumulative cursor so a subsequent CheckNewAnswer only
        # reports rows added *after* this CheckAnswer / CheckAnswerByFunc.
        # Mirrors Scala's fetchStreamAnswer which always updates
        # lastFetchedMemorySinkLastBatchId.
        self._last_fetched_rows = len(rows)
        return rows

    def _poll_sink(self, timeout_ms: int) -> List[Row]:
        """Snapshot the sink rows without waiting on ``processAllAvailable``.

        Used by ``CheckAnswerWithTimeout`` / ``CheckAnswerContainsWithTimeout``
        for RTM tests where ``processAllAvailable`` is not meaningful.
        ``timeout_ms`` is unused here; the caller drives the polling loop.
        """
        if self.custom_sink is not None:
            return self.custom_sink.all_data()
        return self.sink_reader.all_data()

    def _fetch_new(self) -> List[Row]:
        """Rows added since the previous Check action consumed the sink.

        Uses positional slicing on ``MemorySink.allData`` (which is in batch
        arrival order) rather than batch ids: the JVM ``MemorySink`` is
        rebuilt on every ``StartStream`` and may merge replayed source
        batches into a single sink batch, so batch ids aren't stable across
        a restart, but the cumulative row order is.
        """
        if self.custom_sink is not None:
            self._fail(
                "CheckNewAnswer is not supported with a custom sink (e.g. "
                "ContinuousMemorySink for RTM); use CheckAnswerWithTimeout "
                "for cumulative checks against the polling sink."
            )
        if self.output_mode != "append":
            # In complete / update modes the MemorySink truncates per batch,
            # so the cumulative row count is not monotone and positional
            # slicing produces wrong (often empty) results. Reject loudly
            # rather than silently returning the empty multiset.
            self._fail(
                f"CheckNewAnswer is not supported with output_mode="
                f"{self.output_mode!r}; the memory sink truncates per batch "
                f"and the cumulative-row cursor cannot be reasoned about. "
                f"Use CheckAnswer (which inspects the full sink) instead."
            )
        # Capture the cursor *before* ``_fetch_all`` advances it.
        previous = self._last_fetched_rows
        all_rows = self._fetch_all()
        return all_rows[previous:]

    def _fetch_last_batch(self) -> List[Row]:
        """Rows produced by the most recently committed batch only.

        "Most recent batch" is scoped to the *current* query -- after a
        ``StartStream`` the sink is reset, so this action never reaches
        back into a previous run. Fails the test when no batch has
        committed yet so the user gets a clear error rather than an
        empty-actual diff.
        """
        if self.custom_sink is not None:
            self._fail(
                "CheckLastBatch / CheckLastBatchByFunc is not supported with "
                "a custom sink (e.g. ContinuousMemorySink for RTM); "
                "ContinuousMemorySink does not expose batch ids. Use "
                "CheckAnswerWithTimeout / CheckAnswerContainsWithTimeout."
            )
        self._verify(self.current_query is not None, "Stream not running")
        self._wait_for_processing()
        latest = self.sink_reader.latest_batch_id()
        if latest is None:
            self._fail(
                "CheckLastBatch / CheckLastBatchByFunc requires at least one "
                "committed batch but the memory sink has none yet. Add "
                "AddData(source, ...) and ProcessAllAvailable() before this "
                "action, or use CheckAnswer for cumulative checks."
            )
        rows = self.sink_reader.data_since_batch(latest - 1)
        # Keep cumulative tracking in sync so a follow-up CheckNewAnswer
        # after CheckLastBatch sees zero new rows. ``_fetch_all`` here is
        # cheap (the wait_for_processing above already drained the query)
        # and is used for its side effect of advancing ``_last_fetched_rows``.
        self._fetch_all()
        return rows

    def _check(self, action: _CheckActionBase, actual: List[Row]) -> None:
        expected = action.resolve_expected(self.output_schema)
        diff = _diff_rows(expected, actual, action.ordered)
        if diff:
            self._fail(diff)

    def _check_with_timeout(
        self,
        action: _CheckActionBase,
        *,
        contains: bool,
    ) -> None:
        """Poll the sink until the expected rows match (or are contained),
        or fail when ``timeout_ms`` expires.

        Used by ``CheckAnswerWithTimeout`` and ``CheckAnswerContainsWithTimeout``
        for RTM tests where ``processAllAvailable`` is not meaningful. Aborts
        early if the query terminates with an exception so failures point at
        the real cause instead of an opaque timeout.
        """
        self._verify(self.current_query is not None, "Stream not running")
        timeout_ms = action.timeout_ms  # type: ignore[attr-defined]
        expected = action.resolve_expected(self.output_schema)
        deadline = time.monotonic() + timeout_ms / 1000.0
        # Cap the poll interval at the default 100 ms but scale down for
        # short timeouts so a sub-100 ms timeout still polls more than once.
        # 5 polls minimum (timeout_ms/5/1000 in seconds), default 100 ms.
        poll_interval = min(0.1, timeout_ms / 5_000.0)
        last_diff: Optional[str] = None
        while time.monotonic() < deadline:
            actual = self._poll_sink(timeout_ms)
            if contains:
                last_diff = _diff_contains(expected, actual)
            else:
                last_diff = _diff_rows(expected, actual, action.ordered)
            if last_diff is None:
                # Advance the cumulative cursor so a follow-up CheckNewAnswer
                # (on the standard path) only sees rows added after this
                # check. RTM (custom_sink) explicitly rejects CheckNewAnswer,
                # but this keeps the standard path's invariant intact for
                # tests that mix the action with format("memory").
                self._last_fetched_rows = len(actual)
                return
            # Fail fast on a dead query -- burning the full timeout while the
            # JVM-side query has crashed turns every RTM bug into an opaque
            # "timed out" report.
            if not self.current_query.isActive:
                exc = self.current_query.exception()
                if exc is not None:
                    self._fail(
                        f"{type(action).__name__}: query terminated with "
                        f"exception during poll: {exc}"
                    )
                # Inactive without exception (e.g. trigger=Once finished) --
                # one more synchronous poll to capture any rows committed in
                # the same instant the query was finishing -- not a "flush
                # window", since there's no sleep here.
                actual = self._poll_sink(timeout_ms)
                last_diff = (
                    _diff_contains(expected, actual) if contains
                    else _diff_rows(expected, actual, action.ordered)
                )
                if last_diff is None:
                    self._last_fetched_rows = len(actual)
                    return
                self._fail(
                    f"{type(action).__name__}: query terminated cleanly "
                    f"before producing the expected rows.\n"
                    f"{last_diff}"
                )
            time.sleep(poll_interval)
        # Final liveness check after the deadline expires: if the query
        # crashed in the gap between the last in-loop check and the
        # deadline, surface that exception rather than reporting an
        # opaque timeout.
        if not self.current_query.isActive:
            exc = self.current_query.exception()
            if exc is not None:
                self._fail(
                    f"{type(action).__name__}: query terminated with "
                    f"exception during poll: {exc}"
                )
        self._fail(
            f"{type(action).__name__} timed out after {timeout_ms}ms.\n"
            f"{last_diff or '<no diff captured>'}"
        )

    # ----- action dispatch ---------------------------------------------

    def _execute_action(self, action: StreamAction) -> None:
        logger.info("StreamTest action %d: %s", self.pos, action)

        if isinstance(action, StartStream):
            self._start_query(
                trigger=action.trigger,
                checkpoint_override=action.checkpoint_location,
                opts=action.extra_options,
            )
            return

        if isinstance(action, StopStream):
            self._stop_query()
            return

        if isinstance(action, AddData):
            action.source.add_data(*action.data)
            return

        if isinstance(action, ProcessAllAvailable):
            self._wait_for_processing()
            return

        if isinstance(action, CheckAnswer):
            self._check(action, self._fetch_all())
            return

        if isinstance(action, CheckLastBatch):
            self._check(action, self._fetch_last_batch())
            return

        if isinstance(action, CheckNewAnswer):
            self._check(action, self._fetch_new())
            return

        if isinstance(action, CheckAnswerByFunc):
            actual = (
                self._fetch_last_batch() if action.last_only else self._fetch_all()
            )
            try:
                action.check_func(actual)
            except AssertionError:
                raise
            except BaseException as e:  # noqa: BLE001
                self._fail(f"CheckAnswerByFunc raised: {e}", e)
            return

        if isinstance(action, CheckAnswerWithTimeout):
            self._check_with_timeout(action, contains=False)
            return

        if isinstance(action, CheckAnswerContainsWithTimeout):
            self._check_with_timeout(action, contains=True)
            return

        if isinstance(action, ExternalAction):
            try:
                action.func()
            except AssertionError:
                raise
            except BaseException as e:  # noqa: BLE001
                self._fail(f"ExternalAction({action.name!r}) raised: {e}", e)
            return

        if isinstance(action, ExpectFailure):
            self._verify(self.current_query is not None, "ExpectFailure: no running stream")
            terminated = False
            try:
                # ``awaitTermination(timeout)`` returns True on termination,
                # False on timeout, and may raise ``StreamingQueryException``
                # on failure depending on PySpark configuration.
                terminated = bool(
                    self.current_query.awaitTermination(self.test.streaming_timeout_seconds)
                )
            except BaseException:
                # The exception path *is* the expected outcome -- the query
                # has terminated with a failure that we'll inspect below.
                terminated = True
            exc = self.current_query.exception()
            if not terminated and exc is None:
                # Query is still running and has no captured exception. The
                # original "completed cleanly" message would mislead a debugger
                # into chasing a non-existent clean termination.
                self._fail(
                    f"ExpectFailure: query did not terminate within "
                    f"{self.test.streaming_timeout_seconds:.1f}s and has no "
                    f"captured exception (still running)."
                )
            self._verify(exc is not None, "Expected stream failure but query completed cleanly")
            assert exc is not None  # for type checkers
            self._verify(
                _exception_matches(exc, action.exception_type),
                f"Expected failure of type {action.exception_type.__name__}, "
                f"got: {exc!r}",
            )
            if action.assert_failure is not None:
                try:
                    action.assert_failure(exc)
                except AssertionError:
                    raise
                except BaseException as e:  # noqa: BLE001
                    self._fail(f"ExpectFailure.assert_failure raised: {e}", e)
            self.last_query = self.current_query
            self.current_query = None
            self.sink_reader.detach()
            return

        if isinstance(action, Assert):
            try:
                ok = bool(action.condition())
            except AssertionError:
                raise
            except BaseException as e:  # noqa: BLE001
                self._fail(f"Assert raised: {action.message}", e)
                return
            self._verify(ok, f"Assert failed: {action.message}")
            return

        if isinstance(action, AssertOnQuery):
            query = self.current_query or self.last_query
            self._verify(query is not None, "AssertOnQuery: no query available")
            try:
                ok = bool(action.condition(query))
            except AssertionError:
                raise
            except BaseException as e:  # noqa: BLE001
                self._fail(f"AssertOnQuery raised: {action.message}", e)
                return
            self._verify(ok, f"AssertOnQuery failed: {action.message}")
            return

        if isinstance(action, Execute):
            query = self.current_query or self.last_query
            self._verify(query is not None, f"Execute({action.name}): no query available")
            try:
                action.func(query)
            except AssertionError:
                raise
            except BaseException as e:  # noqa: BLE001
                self._fail(f"Execute({action.name}) raised: {e}", e)
            return

        self._fail(f"Unknown action type: {type(action).__name__}")

    # ----- top-level loop ----------------------------------------------

    def run(self) -> None:
        try:
            for self.pos, action in enumerate(self.actions):
                self._execute_action(action)
        except AssertionError:
            raise
        except BaseException as e:  # noqa: BLE001
            self._fail(f"Unexpected error at action {self.pos}", e)
        finally:
            if self.current_query is not None:
                try:
                    if self.current_query.isActive:
                        self.current_query.stop()
                        if not self.current_query.awaitTermination(
                            self.test.streaming_timeout_seconds
                        ):
                            logger.warning(
                                "Query %r did not terminate within %.1fs during "
                                "teardown; checkpoint dir may not be cleaned up",
                                self.query_name,
                                self.test.streaming_timeout_seconds,
                            )
                except Exception:
                    logger.exception("Failed to stop query during teardown")
            self.sink_reader.detach()
            try:
                shutil.rmtree(self.checkpoint_dir, ignore_errors=True)
            except Exception:
                logger.exception(
                    "Failed to remove checkpoint dir %s", self.checkpoint_dir
                )
            try:
                # Drop the memory sink table so subsequent tests don't see
                # rows from this test.
                self.test.spark.sql(f"DROP TABLE IF EXISTS {self.query_name}")
            except Exception:
                logger.exception("Failed to drop sink table %s", self.query_name)


def _diff_contains(expected: List[Row], actual: List[Row]) -> Optional[str]:
    """Multiset subset check: every expected row must appear in ``actual``.

    Returns ``None`` when ``expected`` is a multiset subset of ``actual``,
    else a formatted error message listing the missing rows.
    """
    try:
        e_counter: Counter = Counter(expected)
        a_counter: Counter = Counter(actual)
        missing = e_counter - a_counter
        missing_rows = list(missing.elements())
    except TypeError:
        actual_remaining = list(actual)
        missing_rows = []
        for row in expected:
            try:
                actual_remaining.remove(row)
            except ValueError:
                missing_rows.append(row)
    if not missing_rows:
        return None
    return (
        f"Expected rows not found in actual output.\n"
        f"Missing ({len(missing_rows)}):\n{_format_rows(missing_rows)}\n"
        f"Actual ({len(actual)} rows):\n{_format_rows(actual)}"
    )


def _exception_matches(exc: BaseException, exc_type: type) -> bool:
    """Heuristic match between a streaming query exception and a class.

    Match strategy, in order:

    1. ``isinstance(exc, exc_type)`` -- covers exceptions whose Python class
       chain already encodes the desired type (e.g. ``StreamingQueryException``).
    2. Walk ``exc.__cause__`` and ``exc.__context__`` chains and try
       ``isinstance`` on each.
    3. As a last resort, search the rendered exception text (which on PySpark
       includes the JVM ``Caused by:`` chain for query-thread failures) for
       the simple or fully-qualified class name as a whole word.

    Step 3 is needed because JVM exceptions surface in
    ``StreamingQueryException`` only as a textual field, not as Python objects
    in the cause chain. A request for the simple name ``ArithmeticException``
    will match ``java.lang.ArithmeticException`` because ``\\b`` honors the
    dot as a word boundary, but will not match ``SparkArithmeticException``
    (those share a word, not a boundary) -- that conservative behavior matches
    the user's literal intent.
    """
    seen: set = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, exc_type):
            return True
        # Prefer __cause__ (explicit `raise X from Y`) over __context__.
        cur = cur.__cause__ or cur.__context__
    # Skip the regex fallback for `Exception` / `BaseException` defaults --
    # almost every JVM/Python error message contains the word "Exception",
    # so the fallback would match anything and silently mask type
    # mismatches. The isinstance walk above already accepts every Exception
    # subclass that surfaces in Python.
    if exc_type in (Exception, BaseException):
        return False
    name = exc_type.__name__
    text = str(exc)
    return bool(re.search(rf"\b{re.escape(name)}\b", text))


__all__ = ["StreamTest"]
