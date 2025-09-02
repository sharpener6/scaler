#!/usr/bin/env python3
"""
Simple test to verify task cancellation is working correctly.
"""
import time
import math
from scaler import Client, SchedulerClusterCombo
from scaler.utility.network_util import get_available_tcp_port

def sleep_and_compute(duration: int, value: int):
    """Function that sleeps then computes to test cancellation."""
    time.sleep(duration)
    return math.sqrt(value)

def test_cancel():
    address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
    cluster = SchedulerClusterCombo(address=address, n_workers=1, event_loop="builtin")
    
    try:
        with Client(address=address) as client:
            # Submit a long-running task
            print("Submitting long-running task...")
            future = client.submit(sleep_and_compute, 5, 16)  # Sleep 5 seconds then compute sqrt(16)
            
            # Immediately try to cancel it
            print("Cancelling task...")
            cancel_result = future.cancel()
            print(f"Cancel returned: {cancel_result}")
            
            # Check if it's cancelled
            print(f"Future cancelled: {future.cancelled()}")
            
            if future.cancelled():
                print("✓ Task was successfully cancelled")
            else:
                print("✗ Task was not cancelled")
                try:
                    result = future.result(timeout=1)
                    print(f"Task completed with result: {result}")
                except Exception as e:
                    print(f"Task failed with exception: {e}")
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    test_cancel()