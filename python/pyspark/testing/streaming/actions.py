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
"""Declarative actions for the PySpark ``StreamTest`` framework.

Each action is a small dataclass-like value object that carries the inputs
needed by the ``StreamTest`` driver loop. The driver matches on action type
and dispatches accordingly. Mirrors Scala's ``StreamAction`` hierarchy.
"""

from __future__ import annotations

from abc import ABC
from typing import Any, Callable, Dict, List, Optional, Type

from pyspark.sql import Row
from pyspark.sql.types import StructType
from pyspark.testing.streaming._conversions import to_rows
from pyspark.testing.streaming.memory_stream import MemoryStream
from pyspark.testing.streaming.rtm import LowLatencyMemoryStream


class StreamAction(ABC):
    """Base class for all stream test actions."""

    @property
    def requires_running_stream(self) -> bool:
        """Whether this action requires the stream to be actively running."""
        return False

    def __repr__(self) -> str:
        return self.__class__.__name__


# ---------------------------------------------------------------------------
# Lifecycle / data
# ---------------------------------------------------------------------------


class StartStream(StreamAction):
    """Start (or restart) the streaming query under test.

    Parameters
    ----------
    trigger : dict, optional
        Keyword args passed to ``DataStreamWriter.trigger``, e.g.
        ``{"processingTime": "0 seconds"}`` or ``{"once": True}``.
    extra_options : dict, optional
        Extra options passed to ``DataStreamWriter.option``.
    checkpoint_location : str, optional
        Override the auto-allocated checkpoint location. Use with care:
        ``MemoryStream`` cannot recover from a checkpoint, so reusing a
        checkpoint that has offset history will fail.
    """

    def __init__(
        self,
        trigger: Optional[Dict[str, Any]] = None,
        extra_options: Optional[Dict[str, str]] = None,
        checkpoint_location: Optional[str] = None,
    ) -> None:
        self.trigger = trigger
        self.extra_options = extra_options
        self.checkpoint_location = checkpoint_location

    def __repr__(self) -> str:
        parts: List[str] = []
        if self.trigger:
            parts.append(f"trigger={self.trigger}")
        if self.extra_options:
            parts.append(f"extra_options={self.extra_options}")
        if self.checkpoint_location:
            parts.append(f"checkpoint_location={self.checkpoint_location}")
        return f"StartStream({', '.join(parts)})"


class StopStream(StreamAction):
    """Stop the currently running query and assert clean shutdown."""

    @property
    def requires_running_stream(self) -> bool:
        return True

    def __repr__(self) -> str:
        return "StopStream()"


class AddData(StreamAction):
    """Append data to a ``MemoryStream`` or ``LowLatencyMemoryStream``.

    Parameters
    ----------
    source : MemoryStream or LowLatencyMemoryStream
        The stream to append to. ``LowLatencyMemoryStream`` is the
        RTM-capable source; the standard ``MemoryStream`` is rejected
        under ``Trigger.RealTime`` queries (see
        :class:`pyspark.testing.streaming.rtm.LowLatencyMemoryStream`).
    *data
        Rows to add. Same flexibility as ``MemoryStream.add_data``.
    """

    def __init__(self, source: Any, *data: Any) -> None:
        if not isinstance(source, (MemoryStream, LowLatencyMemoryStream)):
            raise TypeError(
                f"AddData source must be a MemoryStream or LowLatencyMemoryStream, "
                f"got {type(source).__name__}"
            )
        self.source = source
        self.data = data

    def __repr__(self) -> str:
        # Truncate long data lists in error messages.
        preview = self.data if len(self.data) <= 6 else (*self.data[:6], "...")
        return f"AddData(source={self.source}, data={preview})"


class ProcessAllAvailable(StreamAction):
    """Block until ``StreamingQuery.processAllAvailable()`` returns."""

    @property
    def requires_running_stream(self) -> bool:
        return True

    def __repr__(self) -> str:
        return "ProcessAllAvailable()"


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------


class ExpectFailure(StreamAction):
    """Wait for the stream to terminate with an exception of the given type.

    Parameters
    ----------
    exception_type : type
        The expected exception class. Matched against the raw class name as it
        appears in the streaming query's exception message (which is
        sufficient for simple JVM/Python class identification across PySpark
        modes).
    assert_failure : callable, optional
        Optional ``(StreamingQueryException) -> None`` callback that runs
        additional assertions on the captured exception.
    """

    def __init__(
        self,
        exception_type: Type[BaseException] = Exception,
        assert_failure: Optional[Callable[[BaseException], None]] = None,
    ) -> None:
        self.exception_type = exception_type
        self.assert_failure = assert_failure

    @property
    def requires_running_stream(self) -> bool:
        return True

    def __repr__(self) -> str:
        return f"ExpectFailure({self.exception_type.__name__})"


# ---------------------------------------------------------------------------
# Assertions / arbitrary callbacks
# ---------------------------------------------------------------------------


class Assert(StreamAction):
    """Assert that an arbitrary zero-arg callable returns truthy.

    Used for cross-checking external state (counters, captured rows from
    a ``foreachBatch``-style sink, etc.).
    """

    def __init__(self, condition: Callable[[], bool], message: str = "") -> None:
        self.condition = condition
        self.message = message

    def __repr__(self) -> str:
        return f"Assert(condition={_callable_label(self.condition)}, message={self.message!r})"


class AssertOnQuery(StreamAction):
    """Assert a predicate against the active ``StreamingQuery``.

    The predicate receives the current query (the most recently
    started/stopped one if no stream is currently running) and must
    return truthy on success.
    """

    def __init__(
        self,
        condition: Callable[[Any], bool],
        message: str = "",
    ) -> None:
        self.condition = condition
        self.message = message

    @property
    def requires_running_stream(self) -> bool:
        # The Scala equivalent allows assertions after StopStream -- we follow
        # the same convention by *not* requiring a running stream here.
        return False

    def __repr__(self) -> str:
        return (
            f"AssertOnQuery(condition={_callable_label(self.condition)}, "
            f"message={self.message!r})"
        )


class Execute(StreamAction):
    """Run an arbitrary callback receiving the current ``StreamingQuery``.

    Failures inside the callback are surfaced with the surrounding test
    state, like any other framework error.
    """

    def __init__(
        self,
        func: Callable[[Any], None],
        name: str = "Execute",
    ) -> None:
        self.func = func
        self.name = name

    @property
    def requires_running_stream(self) -> bool:
        return False

    def __repr__(self) -> str:
        return f"Execute(func={_callable_label(self.func)}, name={self.name!r})"


def _callable_label(fn: Any) -> str:
    """Best-effort short label for ``fn`` to surface in error messages.

    Reports ``module.qualname`` when available, otherwise falls back to
    ``<lambda at file:line>`` or ``<callable>``.
    """
    qualname = getattr(fn, "__qualname__", None)
    module = getattr(fn, "__module__", None)
    code = getattr(fn, "__code__", None)
    if code is not None:
        loc = f"{code.co_filename}:{code.co_firstlineno}"
    else:
        loc = "<unknown>"
    if qualname and not qualname.endswith("<lambda>") and module:
        return f"{module}.{qualname} @ {loc}"
    if qualname == "<lambda>" or (qualname and qualname.endswith("<lambda>")):
        return f"<lambda @ {loc}>"
    return repr(fn)


# ---------------------------------------------------------------------------
# Check actions -- verify sink contents
# ---------------------------------------------------------------------------


class _CheckActionBase(StreamAction):
    """Common base for ``CheckAnswer`` / ``CheckLastBatch`` / ``CheckNewAnswer``.

    Expected values are not converted to ``Row`` until the driver knows the
    output schema of the streaming DataFrame. This mirrors Scala's lazy
    encoder-driven conversion.
    """

    def __init__(self, *data: Any, ordered: bool = False) -> None:
        self._raw_data: List[Any] = list(data)
        # Renamed from ``sorted`` to ``ordered`` to avoid shadowing the
        # built-in ``sorted`` callable inside this module.
        #
        # ``ordered=True`` requires a deterministic row order. Multi-partition
        # writes commit to the memory sink in the order tasks finish, which
        # is not stable across runs. Set ``spark.sql.shuffle.partitions=1``
        # and ensure a single source partition before relying on ordered=True.
        self.ordered = ordered

    @property
    def requires_running_stream(self) -> bool:
        return True

    def resolve_expected(self, output_schema: StructType) -> List[Row]:
        return to_rows(self._raw_data, output_schema)

    @property
    def raw_data(self) -> List[Any]:
        return self._raw_data


class CheckAnswer(_CheckActionBase):
    """Assert that all sink rows match ``data`` (multiset equality).

    Blocks until the stream finishes processing currently buffered input,
    then collects all rows from the memory sink and compares against the
    expected list. Pass ``ordered=True`` to require ordered equality.

    Accepts the same flexible item types as ``AddData`` /
    ``MemoryStream.add_data`` -- scalars, tuples, dicts, ``Row`` instances,
    namedtuples, dataclasses, plain objects.
    """

    def __repr__(self) -> str:
        return f"CheckAnswer({self._raw_data}, ordered={self.ordered})"


class CheckLastBatch(_CheckActionBase):
    """Assert that the rows produced by the most recent micro-batch match.

    The driver consults the JVM ``MemorySink``'s ``latestBatchId`` so this
    action only inspects rows attributable to the most recent committed
    batch, not the cumulative sink state. ``CheckLastBatch`` is scoped to
    the *current* run: ``StartStream`` always yields a fresh sink, so the
    "most recent batch" never refers back to a previous run. Fails with a
    clear message if no batch has committed yet.
    """

    def __repr__(self) -> str:
        return f"CheckLastBatch({self._raw_data}, ordered={self.ordered})"


class CheckNewAnswer(_CheckActionBase):
    """Assert that rows added since the previous check match.

    Stateful: the driver tracks the cumulative number of sink rows already
    consumed by previous ``CheckNewAnswer`` / ``CheckLastBatch`` calls and
    only returns rows past that count. This counter is preserved across
    ``StopStream`` + ``StartStream`` so replayed rows from a checkpoint
    wipe are not double-reported.
    """

    def __repr__(self) -> str:
        return f"CheckNewAnswer({self._raw_data}, ordered={self.ordered})"


class CheckAnswerByFunc(StreamAction):
    """Validate sink rows with a custom function.

    Mirrors Scala's ``CheckAnswer(globalCheckFunction: Seq[Row] => Unit)``
    overload. The function is called with a list of ``Row`` and must raise
    on failure (assertion errors are surfaced as test failures).

    Parameters
    ----------
    check_func : callable
        ``(List[Row]) -> None``. Raise to signal failure.
    last_only : bool, default False
        If True, only the rows from the most recent batch are passed,
        matching ``CheckLastBatch``'s scope.
    """

    def __init__(
        self,
        check_func: Callable[[List[Row]], None],
        last_only: bool = False,
    ) -> None:
        self.check_func = check_func
        self.last_only = last_only

    @property
    def requires_running_stream(self) -> bool:
        return True

    def __repr__(self) -> str:
        kind = "CheckLastBatchByFunc" if self.last_only else "CheckAnswerByFunc"
        return f"{kind}()"


# ---------------------------------------------------------------------------
# RTM (Real-Time Mode) actions
# ---------------------------------------------------------------------------


class CheckAnswerWithTimeout(_CheckActionBase):
    """Poll the sink until the expected rows match, or fail after ``timeout_ms``.

    Used with ``trigger={"realTime": "<duration>"}`` and a
    :class:`pyspark.testing.streaming.rtm.ContinuousMemorySink`. RTM batches
    run on fixed time intervals so ``processAllAvailable`` is not meaningful;
    this action repeatedly fetches the sink and compares against the
    expected rows until either the multiset matches or the timeout expires.

    Mirrors Scala's ``CheckAnswerRowsNoWait`` /
    ``CheckAnswerRowsContainsWithTimeout`` family of actions.

    Parameters
    ----------
    timeout_ms : int
        Maximum time in milliseconds to poll for the expected answer.
    *data
        Expected rows; same flexible types as ``CheckAnswer``.
    """

    def __init__(self, timeout_ms: int, *data: Any, ordered: bool = False) -> None:
        super().__init__(*data, ordered=ordered)
        if timeout_ms <= 0:
            raise ValueError(f"timeout_ms must be > 0, got {timeout_ms}")
        if not data:
            raise ValueError(
                "CheckAnswerWithTimeout requires at least one expected row "
                "(an empty multiset matches the empty sink instantly, which "
                "is almost never the user's intent for a polling check)."
            )
        self.timeout_ms = int(timeout_ms)

    def __repr__(self) -> str:
        return (
            f"CheckAnswerWithTimeout(timeout_ms={self.timeout_ms}, "
            f"data={self._raw_data})"
        )


class CheckAnswerContainsWithTimeout(_CheckActionBase):
    """Poll the sink until ``data`` is a multiset *subset* of the actual rows.

    Like :class:`CheckAnswerWithTimeout`, but the expected rows must merely
    be present -- extra rows in the sink are tolerated. RTM batches can flush
    additional rows beyond the assertion's interest; this action handles
    that by checking subset membership rather than equality.

    Mirrors Scala's ``CheckAnswerRowsContainsWithTimeout``.
    """

    def __init__(self, timeout_ms: int, *data: Any) -> None:
        super().__init__(*data, ordered=False)
        if timeout_ms <= 0:
            raise ValueError(f"timeout_ms must be > 0, got {timeout_ms}")
        if not data:
            raise ValueError(
                "CheckAnswerContainsWithTimeout requires at least one "
                "expected row (an empty multiset is trivially a subset)."
            )
        self.timeout_ms = int(timeout_ms)

    def __repr__(self) -> str:
        return (
            f"CheckAnswerContainsWithTimeout(timeout_ms={self.timeout_ms}, "
            f"data={self._raw_data})"
        )


class ExternalAction(StreamAction):
    """Execute an arbitrary side-effecting zero-arg callable.

    Like :class:`Execute` but does not receive the streaming query -- useful
    for sleeping, advancing a manual clock, or triggering external state
    needed by the test (mirrors Scala's ``ExternalAction``).
    """

    def __init__(self, func: Callable[[], None], name: str = "ExternalAction") -> None:
        self.func = func
        self.name = name

    def __repr__(self) -> str:
        return f"ExternalAction(func={_callable_label(self.func)}, name={self.name!r})"


class Sleep(ExternalAction):
    """Sleep for ``seconds`` seconds.

    Convenience wrapper around :class:`ExternalAction` for RTM tests that
    need to let real wall-clock time pass for batch boundaries to fire.
    Avoid in non-RTM tests; use ``ProcessAllAvailable`` instead.
    """

    def __init__(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError(f"seconds must be >= 0, got {seconds}")
        self.seconds = float(seconds)

        import time as _time
        super().__init__(lambda: _time.sleep(self.seconds), name=f"Sleep({self.seconds}s)")

    def __repr__(self) -> str:
        return f"Sleep({self.seconds}s)"


__all__ = [
    "StreamAction",
    "StartStream",
    "StopStream",
    "AddData",
    "ProcessAllAvailable",
    "ExpectFailure",
    "Assert",
    "AssertOnQuery",
    "Execute",
    "CheckAnswer",
    "CheckLastBatch",
    "CheckNewAnswer",
    "CheckAnswerByFunc",
    "CheckAnswerWithTimeout",
    "CheckAnswerContainsWithTimeout",
    "ExternalAction",
    "Sleep",
]
