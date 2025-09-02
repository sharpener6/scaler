#!/usr/bin/env python3

import time
import math
from scaler import Client, SchedulerClusterCombo
from concurrent.futures import CancelledError
from scaler.utility.network_util import get_available_tcp_port

def noop_sleep(sec: int):
    time.sleep(sec)
    return sec

def test_cancel_scenarios():
    print("Testing cancel scenarios...")
    address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
    cluster = SchedulerClusterCombo(address=address, n_workers=3, event_loop="builtin")
    
    try:
        with Client(address=address) as client:
            # Test 1: Cancel before task starts
            print("Test 1: Quick cancel of sqrt task")
            fut = client.submit(math.sqrt, 100.0)
            cancelled = fut.cancel()
            print(f"Cancel returned: {cancelled}")
            print(f"Future cancelled: {fut.cancelled()}")
            print(f"Future done: {fut.done()}")
            
            try:
                result = fut.result()
                print(f"Result: {result}")
            except CancelledError:
                print("Got CancelledError as expected")
            
            # Test 2: Cancel after task completes
            print("\nTest 2: Cancel after task completes")
            fut2 = client.submit(math.sqrt, 16)
            result = fut2.result()
            print(f"Result: {result}")
            cancelled2 = fut2.cancel()
            print(f"Cancel after completion returned: {cancelled2}")
            print(f"Future cancelled after completion: {fut2.cancelled()}")
            
            # Test 3: Cancel long running task
            print("\nTest 3: Cancel long running task")
            fut3 = client.submit(noop_sleep, 5)
            time.sleep(0.1)  # Give it a moment to start
            cancelled3 = fut3.cancel()
            print(f"Cancel long task returned: {cancelled3}")
            print(f"Future cancelled: {fut3.cancelled()}")
            try:
                result3 = fut3.result()
                print(f"Result: {result3}")
            except CancelledError:
                print("Got CancelledError as expected")
            
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    test_cancel_scenarios()