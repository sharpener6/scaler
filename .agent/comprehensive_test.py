#!/usr/bin/env python3

"""
Comprehensive test to verify TaskCancelConfirm semantics as described:

When user calls future.cancel():
1. It sends TaskCancel to scheduler
2. Scheduler responds with either:
   - TaskCancelConfirm (Canceled/CancelFailed/CancelNotFound)
   - TaskResult (if task completed before cancel could take effect)
3. According to new semantics: if cancel() is called, the future should be 
   marked as cancelled even if TaskResult arrives after the cancel request
"""

import time
import threading
from concurrent.futures import CancelledError

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port


def instant_task(x):
    """Task that completes immediately"""
    return x * x


def slow_task(x):
    """Task that takes time to complete"""  
    time.sleep(0.5)
    return x * x


def very_slow_task(x):
    """Task that takes a long time"""
    time.sleep(3)
    return x * x


def test_cancel_semantics():
    """Test the new cancel semantics"""
    setup_logger()
    
    address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
    cluster = SchedulerClusterCombo(address=address, n_workers=3, event_loop="builtin")
    
    try:
        with Client(address=address) as client:
            print("=== Test 1: Cancel completed task (new semantics) ===")
            # Submit instant task and wait for completion
            future1 = client.submit(instant_task, 4)
            result = future1.result()  # Wait for completion
            print(f"Task completed with result: {result}")
            print(f"Future done before cancel: {future1.done()}")
            print(f"Future cancelled before cancel: {future1.cancelled()}")
            
            # Now cancel - should mark as cancelled per new semantics
            cancel_result = future1.cancel()
            print(f"Cancel returned: {cancel_result}")
            print(f"Future cancelled after cancel: {future1.cancelled()}")
            print(f"Future done after cancel: {future1.done()}")
            
            # Try to get result - should raise CancelledError
            try:
                result2 = future1.result()
                print(f"ERROR: Got result {result2} instead of CancelledError")
            except CancelledError:
                print("✓ Got CancelledError as expected (new semantics working)")
            except Exception as e:
                print(f"ERROR: Got unexpected exception: {e}")
            
            print()
            
            print("=== Test 2: Cancel running task ===")
            # Submit slow task
            future2 = client.submit(very_slow_task, 5)
            time.sleep(0.1)  # Let task start
            print(f"Future running: {future2.running()}")
            
            # Cancel while running
            cancel_result = future2.cancel()
            print(f"Cancel returned: {cancel_result}")
            print(f"Future cancelled: {future2.cancelled()}")
            
            try:
                result = future2.result(timeout=1)
                print(f"Task completed with result: {result} (cancel failed)")
            except CancelledError:
                print("✓ Task was cancelled successfully")
            except Exception as e:
                print(f"Got exception: {e}")
                
            print()
            
            print("=== Test 3: Multiple rapid cancels ===")
            future3 = client.submit(slow_task, 6)
            
            # Try multiple cancels
            cancel1 = future3.cancel()
            cancel2 = future3.cancel()  # Should return immediately since already cancelled
            
            print(f"First cancel returned: {cancel1}")
            print(f"Second cancel returned: {cancel2}")
            print(f"Final cancelled state: {future3.cancelled()}")
            
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    test_cancel_semantics()