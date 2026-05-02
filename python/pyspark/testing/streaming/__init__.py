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
"""A declarative testing framework for PySpark Structured Streaming.

This is the Python port of Scala's ``StreamTest`` -- see
``sql/core/src/test/scala/org/apache/spark/sql/streaming/StreamTest.scala``
for the reference implementation. Tests are written as a sequence of
actions executed against a streaming DataFrame::

    from pyspark.testing.streaming import (
        StreamTest, MemoryStream, AddData, ProcessAllAvailable,
    )

    class MyStreamingTest(StreamTest):
        def test_smoke(self):
            source = MemoryStream(self.spark, "int")
            self.run_stream_test(
                source.to_df(),
                AddData(source, 1, 2, 3),
                ProcessAllAvailable(),
            )
"""

from pyspark.testing.streaming.actions import (
    AddData,
    Assert,
    AssertOnQuery,
    Execute,
    ExpectFailure,
    ProcessAllAvailable,
    StartStream,
    StopStream,
    StreamAction,
)
from pyspark.testing.streaming.memory_stream import MemoryStream
from pyspark.testing.streaming.stream_test import StreamTest

__all__ = [
    "StreamTest",
    "StreamAction",
    "MemoryStream",
    "AddData",
    "StartStream",
    "StopStream",
    "ProcessAllAvailable",
    "ExpectFailure",
    "Assert",
    "AssertOnQuery",
    "Execute",
]
