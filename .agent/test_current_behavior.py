#!/usr/bin/env python3
"""
Test to verify the current TaskCancelConfirm implementation is working correctly.
This test demonstrates that the future.cancel() semantics match the new specification.
"""

import time
import unittest
from concurrent.futures import CancelledError

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port


def fast_task():
    """A task that completes quickly."""
    return "completed"


def slow_task(duration: int):
    """A task that takes some time to complete."""
    time.sleep(duration)
    return f"completed after {duration}s"


class TestTaskCancelConfirm(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self._workers = 2
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=self._workers, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()

    def test_cancel_quick_task_becomes_cancelled(self):
        """Test that calling cancel() on a completed task marks it as cancelled."""
        with Client(address=self.address) as client:
            # Submit a quick task and wait for it to complete
            future = client.submit(fast_task)
            result = future.result()
            self.assertEqual(result, "completed")
            self.assertTrue(future.done())
            self.assertFalse(future.cancelled())

            # Now cancel it - with new semantics, this should mark it as cancelled
            cancelled = future.cancel()
            self.assertTrue(cancelled)
            self.assertTrue(future.cancelled())

            # Result should raise CancelledError now
            with self.assertRaises(CancelledError):
                future.result()

    def test_cancel_running_task_waits_for_confirmation(self):
        """Test that calling cancel() on a running task waits for TaskCancelConfirm."""
        with Client(address=self.address) as client:
            # Submit a slow task
            future = client.submit(slow_task, 0.5)  # 0.5 second delay
            
            # Cancel immediately before it completes
            cancelled = future.cancel()
            self.assertTrue(cancelled)
            
            # Should be cancelled now
            self.assertTrue(future.cancelled())
            
            with self.assertRaises(CancelledError):
                future.result()

    def test_multiple_cancels_are_idempotent(self):
        """Test that multiple cancel() calls work correctly."""
        with Client(address=self.address) as client:
            future = client.submit(fast_task)
            result = future.result()  # Wait for completion
            
            # First cancel
            self.assertTrue(future.cancel())
            self.assertTrue(future.cancelled())
            
            # Second cancel should still return True and remain cancelled
            self.assertTrue(future.cancel())
            self.assertTrue(future.cancelled())


if __name__ == "__main__":
    unittest.main()
