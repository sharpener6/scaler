#!/usr/bin/env python3

import math
import time
import threading
from concurrent.futures import CancelledError

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port


def noop_sleep(sec: int):
    time.sleep(sec)


def test_cancel_scenarios():
    """Test the specific TaskCancelConfirm scenarios described in the issue."""
    setup_logger()
    
    address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
    cluster = SchedulerClusterCombo(address=address, n_workers=3, event_loop="builtin")
    
    try:
        with Client(address=address) as client:
            print("=== Test 1: Cancel before task completion ===")
            fut1 = client.submit(noop_sleep, 2)
            time.sleep(0.1)  # Give task time to start
            cancel_result = fut1.cancel()
            print(f"Cancel result: {cancel_result}")
            print(f"Future cancelled: {fut1.cancelled()}")
            print(f"Future done: {fut1.done()}")
            
            try:
                result = fut1.result(timeout=5)
                print(f"Unexpected result: {result}")
            except CancelledError:
                print("Got expected CancelledError")
            except Exception as e:
                print(f"Got unexpected exception: {e}")
            
            print("=== Test 2: Cancel after task completion ===")
            fut2 = client.submit(math.sqrt, 16)
            result = fut2.result()
            print(f"Task result: {result}")
            print(f"Future done before cancel: {fut2.done()}")
            
            # With new TaskCancelConfirm semantics, this should mark the future as cancelled
            cancel_result = fut2.cancel()
            print(f"Cancel after completion result: {cancel_result}")
            print(f"Future cancelled after cancel: {fut2.cancelled()}")
            print(f"Future done after cancel: {fut2.done()}")
            
            print("=== Test 3: Cancel very fast task ===")
            fut3 = client.submit(math.sqrt, 25)
            # Try to cancel immediately
            cancel_result = fut3.cancel()
            print(f"Immediate cancel result: {cancel_result}")
            
            # Check final state
            time.sleep(0.5)  # Give time for any pending operations
            print(f"Fast task - cancelled: {fut3.cancelled()}")
            print(f"Fast task - done: {fut3.done()}")
            
            if fut3.cancelled():
                try:
                    result = fut3.result()
                    print(f"Unexpected result from cancelled future: {result}")
                except CancelledError:
                    print("Got expected CancelledError for fast task")
            else:
                result = fut3.result()
                print(f"Fast task completed with result: {result}")
            
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    test_cancel_scenarios()