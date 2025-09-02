#!/usr/bin/env python3

import math
import time
import threading
from concurrent.futures import CancelledError

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port

def long_running_task(sleep_time: float):
    """A task that takes time to complete"""
    time.sleep(sleep_time)
    return sleep_time

def quick_task():
    """A task that completes quickly"""
    return 42

def main():
    setup_logger()
    
    address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
    cluster = SchedulerClusterCombo(address=address, n_workers=3, event_loop="builtin")
    
    try:
        with Client(address=address) as client:
            print("=== Testing cancel on running task ===")
            # Test 1: Cancel a running task
            future1 = client.submit(long_running_task, 2.0)
            time.sleep(0.1)  # Let task start
            
            print(f"Future running: {future1.running()}")
            print(f"Future done: {future1.done()}")
            
            cancel_result = future1.cancel()
            print(f"Cancel returned: {cancel_result}")
            print(f"Future cancelled: {future1.cancelled()}")
            print(f"Future done: {future1.done()}")
            
            try:
                result = future1.result(timeout=1)
                print(f"Result: {result}")
            except CancelledError:
                print("Got CancelledError as expected")
            except Exception as e:
                print(f"Got unexpected exception: {e}")
                
            print()
            
            print("=== Testing cancel on completed task ===")
            # Test 2: Cancel a completed task (new semantics)
            future2 = client.submit(quick_task)
            result2 = future2.result()  # Wait for completion
            print(f"Task completed with result: {result2}")
            
            print(f"Future done: {future2.done()}")
            print(f"Future cancelled (before cancel): {future2.cancelled()}")
            
            # According to new semantics, this should mark as cancelled
            cancel_result2 = future2.cancel()
            print(f"Cancel returned: {cancel_result2}")
            print(f"Future cancelled (after cancel): {future2.cancelled()}")
            print(f"Future done: {future2.done()}")
            
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()