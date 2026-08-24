import time
import unittest

from volnux import EventBase
from volnux.executors.remote_executor import RemoteExecutor
from volnux.telemetry import (
    get_failed_events,
    get_failed_network_ops,
    get_metrics,
    get_retry_stats,
    get_slow_events,
    get_slow_network_ops,
    monitor_events,
)
from volnux.telemetry.factory import TelemetryLoggerFactory
from volnux.telemetry.logger import StandardTelemetryLogger
from volnux.telemetry.network import network_telemetry


# TODO: add tests for batch pipeline cases
class TestTelemetry(unittest.TestCase):
    def setUp(self):
        # Create a fresh factory instance for each test
        TelemetryLoggerFactory._instance = None
        TelemetryLoggerFactory.set_logger_class(StandardTelemetryLogger)
        network_telemetry._metrics.clear()

    def test_event_tracking(self):
        telemetry = TelemetryLoggerFactory.get_logger()
        task_id = "test-task-1"
        event_name = "TestEvent"

        telemetry.start_event(event_name, task_id)
        time.sleep(0.1)  # Simulate work
        telemetry.end_event(task_id, event_name)

        metrics = telemetry.get_metrics(event_name=event_name)
        self.assertIsNotNone(metrics)
        assert metrics is not None  # done for lsp issues
        self.assertEqual(metrics.event_name, "TestEvent")
        self.assertEqual(metrics.status, "completed")
        self.assertGreater(metrics.duration(), 0)

    def test_failed_event_tracking(self):
        telemetry = TelemetryLoggerFactory.get_logger()
        task_id = "test-task-2"
        event_name = "FailedEvent"

        telemetry.start_event(event_name, task_id)
        telemetry.end_event(task_id, event_name, error="Test error")

        metrics = telemetry.get_metrics(event_name=event_name)
        assert metrics is not None  # done for lsp issues
        self.assertEqual(metrics.status, "failed")
        self.assertEqual(metrics.error, "Test error")

        failed_events = get_failed_events()
        self.assertEqual(len(failed_events), 1)
        self.assertEqual(failed_events[0]["event_name"], "FailedEvent")

    def test_retry_tracking(self):
        telemetry = TelemetryLoggerFactory.get_logger()
        task_id = "test-task-3"
        event_name = "RetryEvent"

        telemetry.start_event(event_name, task_id)
        telemetry.record_retry(task_id, event_name)
        telemetry.record_retry(task_id, event_name)
        telemetry.end_event(task_id, event_name)

        metrics = telemetry.get_metrics(event_name=event_name)
        self.assertIsNotNone(metrics)
        assert metrics is not None  # done for lsp issues
        self.assertEqual(metrics.retry_count, 2)

        retry_stats = get_retry_stats()
        self.assertEqual(retry_stats["total_retries"], 2)
        self.assertEqual(retry_stats["events_with_retries"], 1)

    def test_network_telemetry(self):
        task_id = "test-network-1"
        network_telemetry.start_operation(task_id, "localhost", 8080)
        time.sleep(0.1)  # Simulate network operation
        network_telemetry.end_operation(task_id, bytes_sent=1000, bytes_received=500)

        metrics = network_telemetry.get_metrics(task_id)
        assert metrics is not None  # done for lsp issues
        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.host, "localhost")
        self.assertEqual(metrics.port, 8080)
        self.assertEqual(metrics.bytes_sent, 1000)
        self.assertEqual(metrics.bytes_received, 500)
        self.assertGreater(metrics.latency(), 0)

    def test_failed_network_operation(self):
        task_id = "test-network-2"
        network_telemetry.start_operation(task_id, "localhost", 8080)
        network_telemetry.end_operation(task_id, error="Connection refused")

        failed_ops = get_failed_network_ops()
        self.assertEqual(len(failed_ops), 1)
        self.assertEqual(failed_ops[task_id].error, "Connection refused")

    def test_metrics_json_format(self):
        telemetry = TelemetryLoggerFactory.get_logger()
        task_id = "test-task-4"
        event_name = "JsonTest"

        telemetry.start_event(event_name, task_id)
        telemetry.end_event(task_id, event_name)

        metrics_json = get_metrics()
        self.assertIn("JsonTest", metrics_json)
        self.assertIn(task_id, metrics_json)
