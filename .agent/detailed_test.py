#!/usr/bin/env python3

import sys
import os
import time
import threading
from concurrent.futures import CancelledError

# Add the scaler directory to path
sys.path.insert(0, '/home/zyin/dev/scaler')

from scaler import Client, SchedulerClusterCombo
from scaler.utility.network_util import get_available_tcp_port

def very_fast_task():
    """A task that completes almost instantly"""
    return "fast_result"

def medium_task():
    """A task that takes a medium amount of time"""
    time.sleep(0.5)
    return "medium_result"

def test_race_conditions():
    """Test race conditions in cancellation"""
    address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
    cluster = SchedulerClusterCombo(address=address, n_workers=2, event_loop="builtin")
    
    try:
        with Client(address=address) as client:
            print("=== Test: Race condition - cancel after task likely completes ===")
            
            # Test multiple scenarios
            for i in range(5):
                print(f"\nIteration {i+1}:")
                future = client.submit(very_fast_task)
                
                # Small delay to let task potentially complete
                time.sleep(0.01)
                
                cancel_result = future.cancel()
                print(f"  Cancel returned: {cancel_result}")
                print(f"  Future cancelled: {future.cancelled()}")
                print(f"  Future done: {future.done()}")
                
                try:
                    result = future.result(timeout=2)
                    print(f"  Result: {result}")
                except CancelledError:
                    print(f"  Got CancelledError")
                except Exception as e:
                    print(f"  Got exception: {e}")
                    
            print("\n=== Test: Multiple rapid cancellations ===")
            futures = []
            for i in range(3):
                future = client.submit(medium_task)
                futures.append(future)
                
            # Cancel all quickly
            for i, future in enumerate(futures):
                cancel_result = future.cancel()
                print(f"Future {i}: cancel returned {cancel_result}")
                
            # Check results
            for i, future in enumerate(futures):
                print(f"Future {i}: cancelled={future.cancelled()}, done={future.done()}")
                try:
                    result = future.result(timeout=3)
                    print(f"Future {i}: got result {result}")
                except CancelledError:
                    print(f"Future {i}: got CancelledError")
                except Exception as e:
                    print(f"Future {i}: got exception {e}")
                    
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    test_race_conditions()