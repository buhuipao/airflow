# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from airflow.traces import TRACEPARENT, TRACESTATE, otel_tracer, utils
from airflow.traces.tracer import Trace
from tests.test_utils.config import env_vars


@pytest.fixture
def name():
    return "test_traces_run"


class TestOtelTrace:
    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.traces.otel_tracer.conf")
    def test_tracer(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            log = logging.getLogger("TestOtelTrace.test_tracer")
            log.setLevel(logging.DEBUG)
            # hijacking airflow conf with pre-defined
            # values
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            # this will enable debug to set - which outputs the result to console
            conf_a.getboolean.return_value = True

            # mocking console exporter with in mem exporter for better assertion
            in_mem_exporter = InMemorySpanExporter()
            exporter.return_value = in_mem_exporter

            tracer = otel_tracer.get_otel_tracer(Trace)
            assert conf_a.get.called
            assert conf_a.getint.called
            assert conf_a.getboolean.called
            with tracer.start_span(span_name="span1") as s1:
                with tracer.start_span(span_name="span2") as s2:
                    s2.set_attribute("attr2", "val2")
                    span2 = json.loads(s2.to_json())
                span1 = json.loads(s1.to_json())
            # assert the two span data
            assert span1["name"] == "span1"
            assert span2["name"] == "span2"
            trace_id = span1["context"]["trace_id"]
            s1_span_id = span1["context"]["span_id"]
            assert span2["context"]["trace_id"] == trace_id
            assert span2["parent_id"] == s1_span_id
            assert span2["attributes"]["attr2"] == "val2"
            assert span2["resource"]["attributes"]["service.name"] == "abc"

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.traces.otel_tracer.conf")
    def test_dag_tracer(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            log = logging.getLogger("TestOtelTrace.test_dag_tracer")
            log.setLevel(logging.DEBUG)
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            # this will enable debug to set - which outputs the result to console
            conf_a.getboolean.return_value = True

            # mocking console exporter with in mem exporter for better assertion
            in_mem_exporter = InMemorySpanExporter()
            exporter.return_value = in_mem_exporter

            now = datetime.now()
            dag_run = MagicMock()

            parent_trace_id = "0af7651916cd43dd8448eb211c80319c"
            parent_span_id = "b7ad6b7169203331"

            dag_run.conf = {
                TRACEPARENT: f"00-{parent_trace_id}-{parent_span_id}-01",
                TRACESTATE: "key1=val1,key2=val2",
            }
            dag_run.dag_id = "dag_id"
            dag_run.run_id = "run_id"
            dag_run.dag_hash = "hashcode"
            dag_run.run_type = "manual"
            dag_run.queued_at = now
            dag_run.start_date = now

            tracer = otel_tracer.get_otel_tracer(Trace)
            with tracer.start_span_from_dagrun(dagrun=dag_run) as s1:
                with tracer.start_span(span_name="span2") as s2:
                    s2.set_attribute("attr2", "val2")
                span1 = json.loads(s1.to_json())
            assert span1["context"]["trace_id"] != f"0x{parent_trace_id}"
            assert span1["links"][1]["context"]["trace_id"] == f"0x{parent_trace_id}"
            assert span1["links"][1]["context"]["span_id"] == f"0x{parent_span_id}"

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.traces.otel_tracer.conf")
    def test_traskinstance_tracer(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            log = logging.getLogger("TestOtelTrace.test_taskinstance_tracer")
            log.setLevel(logging.DEBUG)
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            # this will enable debug to set - which outputs the result to console
            conf_a.getboolean.return_value = True

            # mocking console exporter with in mem exporter for better assertion
            in_mem_exporter = InMemorySpanExporter()
            exporter.return_value = in_mem_exporter

            now = datetime.now()
            # magic mock
            ti = MagicMock()
            ti.dag_run.conf = {}
            ti.task_id = "task_id"
            ti.start_date = now
            ti.dag_run.dag_id = "dag_id"
            ti.dag_run.run_id = "run_id"
            ti.dag_run.dag_hash = "hashcode"
            ti.dag_run.run_type = "manual"
            ti.dag_run.queued_at = now
            ti.dag_run.start_date = now

            tracer = otel_tracer.get_otel_tracer(Trace)
            with tracer.start_span_from_taskinstance(ti=ti, span_name="mydag") as s1:
                with tracer.start_span(span_name="span2") as s2:
                    s2.set_attribute("attr2", "val2")
                    span2 = json.loads(s2.to_json())
                span1 = json.loads(s1.to_json())

            log.info(span1)
            log.info(span2)
            assert span1["context"]["trace_id"] == f"0x{utils.gen_trace_id(ti.dag_run)}"
            assert span1["context"]["span_id"] == f"0x{utils.gen_span_id(ti)}"

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.traces.otel_tracer.conf")
    def test_span_id_override_is_one_off_and_isolated(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            log = logging.getLogger("TestOtelTrace.test_span_id_override_is_one_off_and_isolated")
            log.setLevel(logging.DEBUG)
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            conf_a.getboolean.return_value = True

            in_mem_exporter = InMemorySpanExporter()
            exporter.return_value = in_mem_exporter

            tracer = otel_tracer.get_otel_tracer(Trace)

            override_span_id_hex = "1111111111111111"
            override_span_id_int = int(override_span_id_hex, 16)

            with tracer.start_span(span_name="override_root", span_id=override_span_id_int) as s_root:
                s_root_json = json.loads(s_root.to_json())
                assert s_root_json["context"]["span_id"] == f"0x{override_span_id_hex}"

                with tracer.start_span(span_name="child_of_override") as s_child:
                    s_child_json = json.loads(s_child.to_json())
                assert s_child_json["context"]["trace_id"] == s_root_json["context"]["trace_id"]
                assert s_child_json["parent_id"] == f"0x{override_span_id_hex}"
                assert s_child_json["context"]["span_id"] != f"0x{override_span_id_hex}"

            with tracer.start_span(span_name="new_root_after_override") as s_new:
                s_new_json = json.loads(s_new.to_json())
            assert s_new_json["context"]["span_id"] != f"0x{override_span_id_hex}"

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.traces.otel_tracer.conf")
    def test_start_span_parent_context_and_span_id_override(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            log = logging.getLogger("TestOtelTrace.test_start_span_parent_context_and_span_id_override")
            log.setLevel(logging.DEBUG)
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            conf_a.getboolean.return_value = True

            in_mem_exporter = InMemorySpanExporter()
            exporter.return_value = in_mem_exporter

            tracer = otel_tracer.get_otel_tracer(Trace)

            parent_trace_id_hex = "0af7651916cd43dd8448eb211c80319c"
            parent_span_id_hex = "b7ad6b7169203331"
            parent_sc = otel_tracer.gen_context(int(parent_trace_id_hex, 16), int(parent_span_id_hex, 16))

            override_child_span_id_hex = "2222222222222222"
            override_child_span_id_int = int(override_child_span_id_hex, 16)

            with tracer.start_span(
                span_name="child_with_parent",
                parent_sc=parent_sc,
                span_id=override_child_span_id_int,
            ) as s_child:
                s_child_json = json.loads(s_child.to_json())

            assert s_child_json["context"]["trace_id"] == f"0x{parent_trace_id_hex}"
            assert s_child_json["parent_id"] == f"0x{parent_span_id_hex}"
            assert s_child_json["context"]["span_id"] == f"0x{override_child_span_id_hex}"

            with tracer.start_span(span_name="fresh_root") as s2:
                s2_json = json.loads(s2.to_json())
            assert s2_json["context"]["span_id"] != f"0x{override_child_span_id_hex}"
            assert s2_json["context"]["trace_id"] != f"0x{parent_trace_id_hex}"

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.traces.otel_tracer.conf")
    def test_dag_tracer_ids_are_computed_and_isolated(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            conf_a.getboolean.return_value = True

            in_mem_exporter = InMemorySpanExporter()
            exporter.return_value = in_mem_exporter

            tracer = otel_tracer.get_otel_tracer(Trace)

            now = datetime.now()
            dag_run1 = MagicMock()
            dag_run1.conf = {}
            dag_run1.dag_id = "dag_a"
            dag_run1.run_id = "run_1"
            dag_run1.queued_at = now
            dag_run1.start_date = now

            with tracer.start_span_from_dagrun(dagrun=dag_run1) as s1:
                s1_json = json.loads(s1.to_json())

            assert s1_json["context"]["trace_id"] == f"0x{utils.gen_trace_id(dag_run1)}"
            assert s1_json["context"]["span_id"] == f"0x{utils.gen_dag_span_id(dag_run1)}"

            later = datetime.now()
            dag_run2 = MagicMock()
            dag_run2.conf = {}
            dag_run2.dag_id = "dag_b"
            dag_run2.run_id = "run_2"
            dag_run2.queued_at = later
            dag_run2.start_date = later

            with tracer.start_span_from_dagrun(dagrun=dag_run2) as s2:
                s2_json = json.loads(s2.to_json())

            assert s2_json["context"]["trace_id"] == f"0x{utils.gen_trace_id(dag_run2)}"
            assert s2_json["context"]["span_id"] == f"0x{utils.gen_dag_span_id(dag_run2)}"
            assert s1_json["context"]["trace_id"] != s2_json["context"]["trace_id"]
            assert s1_json["context"]["span_id"] != s2_json["context"]["span_id"]

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.traces.otel_tracer.conf")
    def test_taskinstance_child_span_parenting_and_ids(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            conf_a.getboolean.return_value = True

            in_mem_exporter = InMemorySpanExporter()
            exporter.return_value = in_mem_exporter

            tracer = otel_tracer.get_otel_tracer(Trace)

            now = datetime.now()
            ti = MagicMock()
            ti.dag_run.conf = {}
            ti.task_id = "task_x"
            ti.try_number = 1
            ti.dag_run.dag_id = "dag_x"
            ti.dag_run.run_id = "run_x"
            ti.dag_run.queued_at = now
            ti.dag_run.start_date = now

            with tracer.start_span_from_taskinstance(ti=ti, child=True, span_name="child") as s_child:
                s_child_json = json.loads(s_child.to_json())

            expected_trace = f"0x{utils.gen_trace_id(ti.dag_run)}"
            expected_parent_span = f"0x{utils.gen_span_id(ti)}"
            assert s_child_json["context"]["trace_id"] == expected_trace
            assert s_child_json["parent_id"] == expected_parent_span
            assert s_child_json["context"]["span_id"] != expected_parent_span
