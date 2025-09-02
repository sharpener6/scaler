#!/usr/bin/env python3

import math
import time
import unittest
from concurrent.futures import CancelledError

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port


def fast_task(x):
    """A task that completes very quickly"""
    return x * x


def slow_task(x):
    """A task that takes some time"""
    time.sleep(2)
    return x * x


class TestCancelSemantics(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self._workers = 3
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=self._workers, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()

    def test_cancel_completed_task_should_mark_cancelled(self):
        """
        Test that when a task completes before cancel confirmation,
        calling cancel() should mark the future as cancelled according to
        the new TaskCancelConfirm semantics.
        """
        with Client(address=self.address) as client:
            # Submit a fast task that will likely complete quickly
            fut = client.submit(fast_task, 4)
            
            # Wait for result to ensure task is complete
            result = fut.result()
            self.assertEqual(result, 16)
            
            # Now call cancel - according to new semantics, this should mark as cancelled
            # even though the task already completed
            cancel_result = fut.cancel()
            self.assertTrue(cancel_result, "cancel() should return True")
            
            # Future should now be cancelled
            self.assertTrue(fut.cancelled(), "Future should be marked as cancelled")
            self.assertTrue(fut.done(), "Future should be done")
            
            # Calling result() should raise CancelledError
            with self.assertRaises(CancelledError):
                fut.result()

    def test_cancel_running_task_waits_for_confirmation(self):
        """
        Test that cancelling a running task waits for TaskCancelConfirm
        """
        with Client(address=self.address) as client:
            # Submit a slow task
            fut = client.submit(slow_task, 4)
            
            # Cancel immediately while task is likely still pending/running
            cancel_result = fut.cancel()
            
            # Should return True indicating cancel was attempted
            self.assertTrue(cancel_result, "cancel() should return True")
            
            # Future should be cancelled (either due to successful cancel or completion during cancel)
            self.assertTrue(fut.cancelled() or fut.done(), "Future should be cancelled or done")


if __name__ == '__main__':
    unittest.main()