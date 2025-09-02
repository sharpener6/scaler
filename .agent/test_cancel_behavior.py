#!/usr/bin/env python3

import math
import time
from concurrent.futures import CancelledError

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port


def main():
    setup_logger()
    address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
    workers = 3
    
    print(f"Starting cluster at {address}")
    
    cluster = SchedulerClusterCombo(address=address, n_workers=workers, event_loop="builtin")
    
    try:
        with Client(address=address) as client:
            print("\n=== Test 1: Cancel a fast task (math.sqrt(100.0)) ===")
            fut = client.submit(math.sqrt, 100.0)
            print(f"Submitted task, task_id: {fut.task_id}")
            
            # Try to cancel immediately
            cancel_success = fut.cancel()
            print(f"Cancel returned: {cancel_success}")
            print(f"Future cancelled: {fut.cancelled()}")
            print(f"Future done: {fut.done()}")
            
            if fut.cancelled():
                print("Future is marked as cancelled")
                try:
                    result = fut.result()
                    print(f"ERROR: Got result when should be cancelled: {result}")
                except CancelledError:
                    print("SUCCESS: CancelledError raised as expected")
                except Exception as e:
                    print(f"ERROR: Unexpected exception: {e}")
            else:
                print("Future is not cancelled")
                try:
                    result = fut.result()
                    print(f"Got result: {result}")
                except Exception as e:
                    print(f"Exception when getting result: {e}")
            
            print("\n=== Test 2: Cancel a completed task ===")
            fut2 = client.submit(math.sqrt, 16)
            result2 = fut2.result()  # Wait for completion
            print(f"Task completed with result: {result2}")
            
            # Now try to cancel the completed task
            cancel_success2 = fut2.cancel()
            print(f"Cancel returned: {cancel_success2}")
            print(f"Future cancelled: {fut2.cancelled()}")
            print(f"Future done: {fut2.done()}")
            
            if fut2.cancelled():
                print("SUCCESS: Completed future marked as cancelled")
            else:
                print("ERROR: Completed future not marked as cancelled")
                
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()